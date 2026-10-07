# Arquitetura do jungle-test

Valores entre parênteses, como `(30s)`, são os padrões da configuração.

## 1. Visão geral

```
   Provedor de jogos                                    Provedor de jogos
         │ HTTP + token                                       │ mensagem
         ▼                                                    ▼
┌──────────────────┐   ┌──────────────┐          ┌──────────────────────────┐
│  API HTTP        │──►│  Keycloak    │          │  SQS                     │
│  (3 instâncias)  │   │  valida token│          │  wager-transactions.fifo │
└────────┬─────────┘   └──────────────┘          └────────────┬─────────────┘
         │                                                    │
         │              ┌──────────────────────┐              │
         └─────────────►│  caso de uso único   │◄─────────────┘
                        │  "processar operação"│
                        └──────────┬───────────┘
                                   │ uma transação SQL
                                   ▼
                        ┌──────────────────────┐
                        │  PostgreSQL          │
                        │  carteira + ledger + │
                        │  inbox + outbox      │
                        └──────────┬───────────┘
                                   │ depois do commit
           ┌───────────────────────┼────────────────────────┐
           ▼                       ▼                        ▼
┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
│ publicador outbox  │  │ worker pendências  │  │ reconciliação      │
│ → wallet-events    │  │ (referência ainda  │  │ (soma o ledger)    │
│   .fifo            │  │  não chegou)       │  │                    │
└────────────────────┘  └────────────────────┘  └────────────────────┘
```

Cada instância roda os mesmos quatro componentes: API, consumidor SQS, publicador do outbox e worker de
pendências. A variável `APP_ROLES` liga ou desliga cada um. Nenhum estado fica na memória: tudo que
importa está no PostgreSQL.

## 2. Camadas e pacotes

```
┌─────────────────────────────────────────────────────────────┐
│ internal/app            monta tudo (Uber Fx)                │
├─────────────────────────────────────────────────────────────┤
│ transport/httpapi   infra/sqsx   worker      ← entradas     │
├─────────────────────────────────────────────────────────────┤
│ application             passo a passo de cada operação      │
├─────────────────────────────────────────────────────────────┤
│ domain                  regras puras (sem HTTP, SQS, banco) │
│   money · wallet · wagering · events                        │
├─────────────────────────────────────────────────────────────┤
│ infra/postgres          SQL explícito com pgx               │
└─────────────────────────────────────────────────────────────┘
```

| Pacote | Conteúdo |
|---|---|
| `domain/money` | tipo `Money`: centavos em `int64` + moeda |
| `domain/wallet` | carteira e linha do ledger |
| `domain/wagering` | operação, máquina de estados, regras de cada tipo, códigos de falha |
| `domain/events` | eventos publicados |
| `application` | casos de uso e interfaces do banco (`TxManager`, repositórios) |
| `infra/postgres` | implementação das interfaces com SQL |
| `infra/sqsx` | leitura da fila de entrada e envio de eventos |
| `transport/httpapi` · `auth` | rotas, respostas e validação do token |
| `worker` | publicador do outbox e worker de pendências |

## 3. Caminho de uma operação

```
pedido ──► token válido? ──não──► 401 / 403
              │ sim
              ▼
          dados válidos? ──não──► 400
              │ sim
              ▼
          chave já usada? ──sim──► mesmo conteúdo? ──sim──► resposta original (replay)
              │ não                       └──não──► 409
              ▼
          trava a carteira (FOR UPDATE)
              ▼
          aplica a regra do tipo
              │
              ├─► aprovada ──────► PROCESSED  (200)
              ├─► recusada ──────► REJECTED   (422)
              └─► falta a referência ► PENDING_REFERENCE (202)
              ▼
          grava tudo junto e confirma (commit)
              operação + ledger + saldo + inbox + eventos
```

### Uma transação, vários repositórios

```
ProcessService (internal/application/process.go)
    │ InTx(ctx, func(repos) error)
    ▼
┌─ uma transação SQL (READ COMMITTED) ───────────────────────────────┐
│ repos.Inbox()         INSERT inbox_messages          (só via SQS)  │
│ repos.Wallets()       SELECT … FOR UPDATE, UPDATE wallets          │
│ repos.Transactions()  INSERT wager_transactions                    │
│ repos.Ledger()        INSERT wallet_ledger_entries                 │
│ repos.Outbox()        INSERT outbox_events                         │
└─────────────────────────────────────── commit ou rollback de tudo ─┘
```

- Biblioteca: `pgx` com SQL escrito à mão em `internal/infra/postgres`, sem ORM. Travas e regras ficam
  visíveis no SQL.
- `TxManager` (`internal/application/ports.go`) abre a transação e entrega os repositórios. Todos usam a
  mesma conexão, então quem decide o que entra no commit é o caso de uso, não cada repositório.
- Deadlock e conflito de serialização: a transação inteira é repetida, até 5 vezes.
- Leituras que precisam de uma foto consistente (reconciliação, ledger) usam `InSnapshot`: transação
  `REPEATABLE READ` só de leitura.

## 4. Dinheiro

| Regra | Detalhe |
|---|---|
| representação | inteiro de centavos (`"25.00"` → `2500`) + moeda ISO 4217 |
| limite | ±92.233.720.368.547.758,07; acima disso dá erro, sem arredondar |
| moedas | só as de 2 casas: BRL, USD, EUR, GBP, ARS, MXN, CAD, AUD, CHF, COP, PEN, UYU |
| operações | somar, subtrair, negar e comparar; moedas diferentes dão erro |
| formato na API | `{"amount":"25.00","currency":"BRL"}`, valor sempre como texto |
| no banco | `BIGINT` em centavos + `CHAR(3)` |

| Entrada | Aceita? |
|---|---|
| `"25.00"`, `"0.00"` | sim |
| `"25"`, `"25.0"`, `"25.000"` | não, precisa de exatamente 2 casas |
| `"1e3"`, `"NaN"`, `"Infinity"`, `"+5.00"`, `"025.00"`, `" 5.00"` | não |
| `25.00` (número, sem aspas) | não |
| `"-1.00"` em entrada externa | não |

## 5. Dados no banco

```
┌──────────────────────┐ 1      N ┌───────────────────────────┐ 1     0..1 ┌────────────────────────┐
│ wallets              │◄─────────│ wager_transactions        │◄───────────│ wallet_ledger_entries  │
├──────────────────────┤          ├───────────────────────────┤            ├────────────────────────┤
│ id                   │          │ id                        │            │ id                     │
│ player_id   ┐ únicos │          │ origin (INTERNAL/EXTERNAL)│            │ wallet_id              │
│ currency    ┘ juntos │          │ kind, status              │            │ transaction_id         │
│ balance_minor  ≥ 0   │          │ amount_minor, currency    │            │ direction DEBIT/CREDIT │
│ version              │          │ provider_id               │            │ amount_minor           │
└──────────────────────┘          │ external_transaction_id   │            │ balance_before_minor   │
                                  │ idempotency_key           │            │ balance_after_minor    │
┌──────────────────────┐          │ payload_hash              │            │ wallet_version         │
│ inbox_messages       │          │ reference_transaction_id ─┼─► outra    └────────────────────────┘
├──────────────────────┤          │ failure_code              │   operação
│ consumer_name ┐ chave│          │ result_balance_minor      │
│ message_id    ┘      │─────────►│ attempts, next_attempt_at │      ┌────────────────────────┐
│ payload_hash         │          │ expires_at                │      │ outbox_events          │
│ deliveries           │          └───────────────────────────┘      ├────────────────────────┤
│ outcome              │                                             │ id (= eventId)         │
└──────────────────────┘                                             │ event_type, payload    │
                                                                     │ group_key (= walletId) │
                                                                     │ attempts, locked_until │
                                                                     │ published_at           │
                                                                     └────────────────────────┘
```

| Tabela | Guarda |
|---|---|
| `wallets` | saldo atual e versão de cada carteira |
| `wager_transactions` | cada operação, com estado, resultado e código de falha |
| `wallet_ledger_entries` | uma linha por movimentação de saldo; só aceita inserção |
| `inbox_messages` | mensagens SQS já tratadas |
| `outbox_events` | eventos esperando ou já publicados |

## 6. Estados de uma operação

```
                         ┌──────────► PROCESSED  ✔ fim
                         │
   PENDING ──────────────┼──────────► REJECTED   ✘ fim
      │                  │
      │                  └──────────► FAILED     ✘ fim
      ▼
   PENDING_REFERENCE ──(tenta de novo)──► PENDING_REFERENCE
      │
      └──► PROCESSED | REJECTED | FAILED
```

| Estado | Quando |
|---|---|
| `PENDING` | recém-criada, ainda em memória durante o processamento |
| `PENDING_REFERENCE` | depende de uma operação que ainda não chegou |
| `PROCESSED` | concluída com sucesso |
| `REJECTED` | recusada por regra de negócio |
| `FAILED` | falha permanente ao retomar uma pendência |

Os três últimos são finais: nem o código nem o banco aceitam mudar depois.

## 7. Tipos de operação

| Tipo | Saldo | Valor | Referência |
|---|---|---|---|
| `BET` | − débito | > 0 | não aceita |
| `WIN` | + crédito | > 0 | opcional; se vier, precisa ser uma BET da mesma rodada |
| `LOSS` | não muda | exatamente `0.00` | não aceita |
| `REFUND` | + crédito | igual ao da BET | obrigatória, uma BET processada |
| `ROLLBACK` | inverso do original | igual ao original | obrigatória: BET, WIN ou REFUND processado |
| `OPENING` | + crédito | > 0 | só interno, criado por `POST /wallets` |

Reversões (`REFUND` e `ROLLBACK`):

```
BET 25,00 ──► REFUND 25,00 ✔   devolve a aposta
    │
    ├──────► ROLLBACK      ✘   REFERENCE_ALREADY_REVERSED (a BET já foi revertida)
    └──────► 2º REFUND     ✘   REFERENCE_ALREADY_REVERSED

REFUND ────► ROLLBACK      ✔   desfaz a devolução; a BET volta a valer e não pode ser devolvida de novo
WIN ───────► ROLLBACK      ✔   se houver saldo; senão REVERSAL_INSUFFICIENT_FUNDS
ROLLBACK ──► ROLLBACK      ✘   INVALID_REFERENCE_KIND
```

Cada operação recebe no máximo uma reversão com sucesso. O banco garante isso com um índice único.

## 8. Idempotência

| Chega uma operação com... | Resultado |
|---|---|
| chave nova | processa |
| mesma chave, mesmo conteúdo | resposta original, `idempotentReplay: true`, saldo da época |
| mesma chave, conteúdo diferente | `409 IDEMPOTENCY_KEY_REUSED` |
| mesmo id externo, outra chave | `409 EXTERNAL_TRANSACTION_ID_REUSED` |
| mesma chave, outro provedor | operação independente |

- Chave: header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS), usada como veio.
- "Conteúdo" = hash SHA-256 de um JSON com chaves em ordem alfabética, feito com: provedor, id externo,
  jogador, carteira, rodada, jogo, tipo, valor, moeda e referência. Ficam fora a chave, headers,
  `messageId` e `occurredAt`.
- Antes do hash, UUIDs vão para letras minúsculas. Valores e textos não são normalizados: só a forma
  canônica é aceita (`"25.00"`, nunca `"25"`), então o hash usa exatamente o texto recebido.
- HTTP e SQS geram o mesmo hash para a mesma operação.
- Unicidade imposta no banco: `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)`.

## 9. Concorrência

Duas apostas de 80,00 sobre 100,00, em instâncias diferentes:

```
   instância A           banco: carteira com 100,00              instância B
    │                               │                                 │
    │── FOR UPDATE ────────────────►│ trava para A                    │
    │                               │◄────────────────── FOR UPDATE ──│ espera
    │── debita 80,00 + commit ─────►│ saldo 20,00, solta              │
    │                               │── trava para B ────────────────►│
    │                               │ lê 20,00 < 80,00                │
    │                               │── REJECTED INSUFFICIENT_FUNDS ─►│
```

| Proteção | Onde |
|---|---|
| trava só a linha da carteira (`SELECT … FOR UPDATE`) | banco; carteiras diferentes não se esperam |
| atualização confere a versão (`… WHERE version = $esperada`) | banco; impede sobrescrever outra gravação |
| `CHECK (balance_minor >= 0)` | banco; barra saldo negativo mesmo com bug |
| repetição automática em deadlock e conflito de unicidade | código |
| espera máxima por trava `(3s)` | sessão do banco; depois disso vira erro temporário |

## 10. Referência que ainda não chegou

```
t=0     REFUND chega, BET não existe ──► PENDING_REFERENCE (202) + evento PendingReference
t=1s    worker tenta: ainda não ─────► agenda +2s
t=3s    worker tenta: ainda não ─────► agenda +4s   (dobra até 30s)
  ...   BET chega ───────────────────► acorda o REFUND na hora
        worker tenta: achou ─────────► PROCESSED
                    ou
t=10min (ou 10 tentativas) ──────────► REJECTED REFERENCE_NOT_FOUND
```

| Situação da referência | Resultado |
|---|---|
| ainda não existe | espera, até o prazo |
| existe mas está pendente | espera; se o prazo acabar, `REFERENCE_NOT_PROCESSED` |
| terminou recusada ou com falha | `REFERENCE_NOT_PROCESSED` na hora |
| jogador, carteira, moeda, rodada ou tipo não batem | recusa na hora (`REFERENCE_MISMATCH` / `INVALID_REFERENCE_KIND`) |

A espera fica gravada no banco (`next_attempt_at`), então qualquer instância continua o trabalho, inclusive
depois de reiniciar.

## 11. Códigos de falha

Recusas gravadas (`status: REJECTED`, HTTP `422`):

| Código | Significa | O provedor pode corrigir? |
|---|---|---|
| `INSUFFICIENT_FUNDS` | aposta maior que o saldo | não |
| `REVERSAL_INSUFFICIENT_FUNDS` | estorno deixaria o saldo negativo | não |
| `REFERENCE_NOT_FOUND` | referência não chegou no prazo | não |
| `REFERENCE_NOT_PROCESSED` | referência foi recusada, falhou ou ficou pendente além do prazo | não |
| `REFERENCE_ALREADY_REVERSED` | a referência já foi revertida | não |
| `BALANCE_OVERFLOW` | crédito passaria do limite do inteiro | não |
| `CURRENCY_MISMATCH` | moeda diferente da carteira | sim, com nova operação |
| `WALLET_PLAYER_MISMATCH` | carteira de outro jogador | sim, com nova operação |
| `REFERENCE_MISMATCH` | referência de outro jogador, carteira, moeda ou rodada | sim, com nova operação |
| `INVALID_REFERENCE_KIND` | tipo de referência não permitido | sim, com nova operação |
| `REVERSAL_AMOUNT_MISMATCH` | valor diferente do original | sim, com nova operação |

Falha permanente gravada (`status: FAILED`, HTTP `500`): `PROCESSING_FAILED`.

Erros que não chegam a virar operação:

| Código | HTTP | Quando |
|---|---|---|
| `INVALID_REQUEST` | 400 | campo faltando ou inválido, header de chave ausente |
| `INTERNAL_KIND_NOT_ALLOWED` | 400 | provedor enviou `OPENING` |
| `UNAUTHENTICATED` | 401 | sem token, token inválido ou vencido |
| `FORBIDDEN` | 403 | sem permissão ou provedor diferente do token |
| `WALLET_NOT_FOUND` | 404 | carteira não existe |
| `TRANSACTION_NOT_FOUND` | 404 | operação não existe ou é de outro provedor |
| `IDEMPOTENCY_KEY_REUSED` | 409 | mesma chave, conteúdo diferente |
| `EXTERNAL_TRANSACTION_ID_REUSED` | 409 | mesmo id externo, outra chave |
| `WALLET_ALREADY_EXISTS` | 409 | já existe carteira para o jogador e a moeda |
| `MESSAGE_ID_REUSED` | DLQ | mesma `messageId` com conteúdo diferente (só SQS) |
| `SERVICE_UNAVAILABLE` | 503 | banco ou fila fora do ar; repetir com a mesma chave |

### Falha temporária ou permanente

| Tipo | Exemplos | O que acontece |
|---|---|---|
| temporária | banco ou fila fora do ar, conexão caída, espera por trava acima de `(3s)`, deadlock, timeout | nada é gravado; HTTP `503` com `Retry-After`; na fila, a mensagem volta mais tarde; repetir com a mesma chave é seguro |
| permanente, da entrada | dado inválido, carteira inexistente, conflito de chave | nada é gravado; HTTP `4xx`; na fila, vai para a DLQ |
| permanente, ao retomar uma pendência | o mesmo erro se repete 5 vezes no worker (dado quebrado, bug) | a operação vira `FAILED` com `PROCESSING_FAILED` e emite `WagerTransactionFailed` |

No PostgreSQL, contam como temporários os erros de conexão (classe `08`), falta de recursos (classe `53`),
banco reiniciando (`57P01` a `57P03`), conflito de serialização (`40001`), deadlock (`40P01`) e espera por
trava esgotada (`55P03`). O código está em `internal/infra/postgres/errors.go`.

## 12. Contrato HTTP

| Rota | Quem chama | Resposta de sucesso |
|---|---|---|
| `POST /wallets` | serviço interno | `201` carteira com `version: 1` |
| `GET /wallets/{id}` | serviço interno | `200` saldo e versão |
| `GET /wallets/{id}/ledger?limit=&cursor=` | serviço interno | `200` linhas em ordem + `nextCursor` |
| `POST /wallets/{id}/reconciliation` | serviço interno | `200` saldo guardado × somado |
| `POST /wagering/transactions` | provedor | `200` / `202` / `422` (corpos abaixo) |
| `GET /wagering/transactions/{id}` | provedor dono ou interno | `200` detalhes da operação |
| `GET /providers/{p}/wagering/transactions/{idExterno}` | provedor dono ou interno | `200` detalhes da operação |
| `GET /health/live`, `/health/ready`, `/metrics` | qualquer um | `200` |

### Respostas de `POST /wagering/transactions`

| HTTP | Situação | Corpo |
|---|---|---|
| `200` | processada, nova ou replay | `{"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}` |
| `202` | esperando a referência | `{"transactionId":"…","status":"PENDING_REFERENCE","idempotentReplay":false}` |
| `422` | recusada por regra de negócio | `{"transactionId":"…","status":"REJECTED","balance":{"amount":"20.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}` |
| `500` | falha permanente já registrada | `{"transactionId":"…","status":"FAILED","failureCode":"PROCESSING_FAILED","idempotentReplay":true}` |
| `400` | entrada inválida | corpo de erro com `INVALID_REQUEST` ou `INTERNAL_KIND_NOT_ALLOWED` e a lista `details` |
| `401` | sem token, token inválido ou vencido | `{"error":{"code":"UNAUTHENTICATED","message":"missing, invalid or expired credentials"},"correlationId":"…"}` e header `WWW-Authenticate: Bearer` |
| `403` | sem permissão ou outro provedor | `{"error":{"code":"FORBIDDEN","message":"operation not allowed for this identity"},"correlationId":"…"}` |
| `404` | carteira não existe | `{"error":{"code":"WALLET_NOT_FOUND","message":"wallet not found"},"correlationId":"…"}` |
| `409` | conflito de idempotência | `{"error":{"code":"IDEMPOTENCY_KEY_REUSED","message":"idempotency key reused with a different payload"},"correlationId":"…"}` |
| `503` | banco ou fila indisponível | `{"error":{"code":"SERVICE_UNAVAILABLE","message":"temporarily unavailable, retry with the same Idempotency-Key"},"correlationId":"…"}` e header `Retry-After: 1` |

- `balance` é o saldo observado quando a operação foi decidida. Na recusa, é o saldo que impediu a operação.
- Um replay devolve o mesmo código HTTP e o mesmo corpo da primeira resposta, com `idempotentReplay: true`.
- `FAILED` só aparece em replay: no caminho direto, um erro inesperado responde `500` sem gravar nada.
- O header `X-Correlation-Id` é aceito e devolvido; se não vier, o serviço gera um.

### Reconciliação

```
POST /wallets/{id}/reconciliation
        │
        ▼  uma foto consistente do banco (REPEATABLE READ, só leitura)
  storedBalance     = saldo guardado em wallets
  calculatedBalance = Σ créditos − Σ débitos do ledger, incluindo a abertura
  difference        = storedBalance − calculatedBalance
  consistent        = difference é zero
        │
        └─ diferente de zero ─► log ERROR + wagering_reconciliation_divergences_total
```

```json
{"walletId":"…","storedBalance":{"amount":"975.00","currency":"BRL"},
 "calculatedBalance":{"amount":"975.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}
```

A reconciliação só lê: nunca altera o saldo.

Corpo de erro (`details` só aparece em erros de validação):

```json
{"error":{"code":"INVALID_REQUEST","message":"invalid request","details":[{"field":"money","reason":"money: amount must have exactly two decimal places: \"25\""}]},"correlationId":"01a1165f-d735-7126-8295-23c1aa8d8a55"}
```

## 13. Fila de entrada (consumidor SQS)

```
mensagem ──► válida? ──não──────────────────────────► DLQ + apaga
               │ sim
               ▼
         messageId já na inbox? ──sim──► mesmo conteúdo? ──sim──► apaga (já feito)
               │ não                           └──não──► DLQ + apaga
               ▼
         processa (mesmo caso de uso do HTTP)
               ├─► sucesso ou recusa de negócio ──► commit, depois apaga
               ├─► erro de entrada (carteira não existe, 409…) ──► DLQ + apaga
               └─► erro temporário ──► volta para a fila mais tarde
                                       (2s, 4s, 8s… até 5min; após 5 recebimentos → DLQ)
```

| Item | Valor |
|---|---|
| fila de entrada | `wager-transactions.fifo` |
| DLQ | `wager-transactions-dlq.fifo` |
| `MessageGroupId` | `walletId`: ordem garantida por carteira, carteiras em paralelo |
| `MessageDeduplicationId` | `messageId` do envelope |
| tempo invisível após leitura | `(30s)` |
| prazo de processamento | `(20s)`, menor que o tempo invisível |
| no desligamento | para de ler, termina o que está em andamento, devolve o resto à fila |

## 14. Eventos publicados (outbox)

```
transação SQL ─► grava operação + evento em outbox_events ─► commit
                                                               │
          publicador (a cada 250ms, em qualquer instância)     │
          pega eventos livres com FOR UPDATE SKIP LOCKED ◄─────┘
          reserva por (30s) ─► envia em lotes de 10 ─► marca published_at
                         │
                         └─ se morrer no meio: a reserva vence e outra instância reenvia,
                            com o mesmo eventId
```

| Evento | Quando |
|---|---|
| `WagerTransactionProcessed` | operação concluída, inclusive `LOSS` e `OPENING` |
| `WagerTransactionRejected` | operação recusada |
| `WalletBalanceChanged` | saldo mudou |
| `WagerTransactionPendingReference` | operação começou a esperar a referência |
| `WagerTransactionFailed` | operação terminou como `FAILED` |

Formato do envelope:

```json
{"eventId":"…","eventType":"WalletBalanceChanged","aggregateType":"Wallet","aggregateId":"<walletId>",
 "correlationId":"…","causationId":"msg-123","occurredAt":"2026-09-08T12:00:00.000Z","version":1,
 "data":{"walletId":"…","transactionId":"…","direction":"DEBIT","money":{"amount":"25.00","currency":"BRL"},
         "balanceBefore":{"amount":"1000.00","currency":"BRL"},"balanceAfter":{"amount":"975.00","currency":"BRL"},
         "walletVersion":2}}
```

Na fila `wallet-events.fifo`, o `MessageGroupId` é o `walletId` e o `MessageDeduplicationId` é o `eventId`.
Quem consome deve ignorar `eventId` repetido e ordenar pelo `walletVersion`.

## 15. O que o banco garante sozinho

| Garantia | Como |
|---|---|
| saldo nunca negativo | `CHECK` |
| uma carteira por jogador e moeda | índice único |
| operação não se repete | índices únicos de chave e id externo |
| um único crédito de abertura por carteira | índice único |
| uma reversão por operação | índice único parcial |
| ledger imutável | triggers bloqueiam `UPDATE`, `DELETE` e `TRUNCATE`, até para o dono |
| saldo e ledger sempre juntos | trigger checado no commit: mudou o saldo, tem que ter a linha no ledger |
| conta do ledger certa | `CHECK balance_after = balance_before ± amount` |
| uma linha de ledger por operação | `UNIQUE (wallet_id, transaction_id)` e chave estrangeira para a operação da mesma carteira |
| versões em sequência | `UNIQUE (wallet_id, wallet_version)`; trigger exige versão +1 a cada mudança de saldo e proíbe mudar a versão sem mudar o saldo |
| ledger só de operação que move dinheiro | trigger checado no commit: recusa linha de `LOSS`, de operação não processada ou com valor diferente |
| abertura interna separada das operações externas | `CHECK`: `OPENING` não tem provedor, id externo, chave, hash, rodada, jogo nem referência, e já nasce `PROCESSED`; operações externas têm todos esses campos |
| política de zero | `CHECK`: `LOSS` vale exatamente zero; os outros tipos, mais que zero |
| reversão aponta para algo | `CHECK`: `REFUND` e `ROLLBACK` sem referência são recusados |
| operação finalizada não muda | trigger |
| evento gravado não muda | trigger |
| aplicação sem poder apagar | usuário `jungle-app` sem permissão de `DELETE`/`TRUNCATE` |

## 16. Segurança

```
Provedor ──(id + senha)──► Keycloak ──► token assinado (5 min)
                                         • papel: wagering-provider
                                         • provider_id: provider-a
                                         • aud: jungle-api
Provedor ──(token)──► API ──► confere assinatura, emissor, audience, validade e tipo
```

| Papel | Pode |
|---|---|
| `wallet-operator` (cliente `wallet-service`) | criar e consultar carteiras, ledger, reconciliação, qualquer operação |
| `wagering-provider` (clientes `provider-a`, `provider-b`) | enviar operações e consultar só as próprias |

- O provedor vem do token. Corpo com outro `providerId` → `403`, antes de qualquer acesso ao banco.
- Operação de outro provedor consultada por id → `404`, sem revelar que existe.
- Na fila, o acesso é controlado por credenciais e políticas do SQS. O consumidor aplica as mesmas
  validações do HTTP.

Por que esse modelo de permissões:

| Decisão | Motivo |
|---|---|
| o provedor vem só do token (`provider_id`, gravado pelo Keycloak) | o corpo não consegue escolher em nome de quem a operação é feita; o enunciado pede que a identidade determine o `providerId` |
| idempotência e referências separadas por provedor | a chave ou a referência de um provedor nunca encontra operações de outro, nem num replay |
| papel próprio para o serviço interno | criar carteira, ver saldo, ledger e reconciliar não ficam ao alcance dos provedores |
| cada papel com o mínimo necessário | provedor não lê carteiras; serviço interno não envia apostas |
| `404` ao consultar por id uma operação de outro provedor | não revela que a operação existe |
| `403` em `/providers/{outro}/…` | o provedor está no próprio caminho, então não há existência a esconder |

Por que o Keycloak:

| Motivo | Detalhe |
|---|---|
| padrão aberto | OAuth 2.0 / OIDC, com `client_credentials` para chamadas entre sistemas |
| claims sob medida | mappers gravam `provider_id` e a audience `jungle-api` no token de cada cliente |
| ambiente reproduzível | o realm vem de um JSON versionado (`deploy/keycloak/realm-jungle.json`); `docker compose up` recria clientes, papéis e segredos |
| sem código de login no serviço | cadastro de senhas e emissão de tokens ficam fora do projeto |

Como o token é conferido (`internal/auth/auth.go`):

- assinatura RS256 contra as chaves públicas do Keycloak, guardadas em cache e buscadas de novo quando
  aparece uma chave desconhecida;
- emissor (`iss`) igual a `OIDC_ISSUER`; o Keycloak sempre escreve `http://localhost:8081/realms/jungle`,
  qualquer que seja o endereço usado para pedir o token;
- audience (`aud`) contém `jungle-api`;
- validade (`exp`) sem tolerância;
- tipo `Bearer`, o que recusa ID tokens.

## 17. Composição e ciclo de vida (Uber Fx)

```
cmd/wagering/main.go
  config.Load()                    valida a configuração antes do Fx; erro → sai com código 2
  fx.New(app.Options(cfg)).Run()   liga tudo e espera SIGTERM ou SIGINT para desligar
```

Os construtores são funções Go comuns que recebem as dependências como parâmetros. O Fx só os liga e
cuida do ciclo de vida; o pacote `domain` não importa o Fx. A montagem fica em `internal/app/app.go`:

| `fx.Module` | O que fornece (`fx.Provide`) | Hook de ciclo de vida |
|---|---|---|
| `observability` | logger JSON, métricas Prometheus | nenhum |
| `postgres` | pool pgx, `TxManager` (via `fx.Annotate` + `fx.As`), store do outbox | ao ligar, testa o banco até responder; ao desligar, fecha o pool |
| `sqs` | um cliente SQS por papel, filas, consumidor, publicador | ao ligar, resolve as URLs das filas, o que confirma que existem |
| `auth` | verificador de tokens OIDC | ao ligar, busca as chaves do Keycloak (só com o papel `api`) |
| `application` | relógio, gerador de UUIDv7, política de pendências, casos de uso | nenhum |
| `workers` | publicador do outbox, worker de pendências; `fx.Invoke(registerWorkers)` | `Start`/`Stop` só dos componentes ligados em `APP_ROLES` (`fx.StartStopHook`) |
| `http` | handlers, readiness, roteador, servidor; `fx.Invoke` do servidor | ao ligar, abre a porta; ao desligar, readiness `503` e `Server.Shutdown` |

O Fx roda os hooks de ligar na ordem das dependências e os de desligar na ordem inversa:

```
ligar     config ─► banco ─► filas ─► workers ─► Keycloak ─► porta HTTP
desligar  porta HTTP ─► consumidor SQS ─► worker pendências ─► publicador ─► banco
```

- Ao ligar, cada dependência é testada dentro de `(60s)`. Se uma falhar, o serviço não sobe e o Fx desfaz
  o que já tinha iniciado.
- Ao desligar, `/health/ready` passa a responder `503` e cada componente termina o trabalho em andamento
  dentro do prazo `(25s)`. Cada worker expõe `Done()`, que fecha quando ele termina.
- `TestGraphIsComplete` valida o grafo de dependências (`fx.ValidateApp`) para cada combinação de
  `APP_ROLES`; `TestFxLifecycleReleasesResources` liga e desliga o serviço e confere que os workers pararam
  e o pool foi fechado.

## 18. Configuração que mais importa

| Variável | Padrão | Para quê |
|---|---|---|
| `APP_ROLES` | `api,consumer,outbox,pending` | componentes ligados na instância |
| `DB_LOCK_TIMEOUT` | `3s` | espera máxima por uma trava |
| `DB_STATEMENT_TIMEOUT` | `5s` | tempo máximo de um comando SQL |
| `SQS_VISIBILITY_TIMEOUT` | `30s` | tempo que a mensagem fica invisível após a leitura |
| `SQS_PROCESS_TIMEOUT` | `20s` | prazo para processar uma mensagem |
| `SQS_MAX_RECEIVES` | `5` | recebimentos antes da DLQ |
| `OUTBOX_POLL_INTERVAL` | `250ms` | frequência do publicador |
| `OUTBOX_LEASE` | `30s` | reserva de um evento por um publicador |
| `REFERENCE_BASE_DELAY` / `REFERENCE_MAX_DELAY` | `1s` / `30s` | espera entre tentativas de uma pendência |
| `REFERENCE_MAX_ATTEMPTS` / `REFERENCE_TTL` | `10` / `10m` | quando desistir de uma pendência |
| `SHUTDOWN_TIMEOUT` | `25s` | prazo para desligar |

Lista completa no [README.md](README.md#variáveis-de-ambiente) e no `.env.example`.

## 19. Observabilidade

| O quê | Onde |
|---|---|
| logs JSON com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId` | saída padrão dos containers |
| operações por resultado | `wagering_transactions_total` |
| duplicatas | `wagering_idempotent_replays_total`, `wagering_inbox_duplicates_total` |
| tentativas e DLQ | `wagering_retries_total`, `wagering_dlq_messages_total` |
| conflitos de concorrência | `wagering_concurrency_conflicts_total` |
| atraso do outbox | `wagering_outbox_lag_seconds`, `wagering_outbox_pending_events` |
| tempo de processamento | `wagering_processing_duration_seconds` |
| saldo divergente | `wagering_reconciliation_divergences_total` |
| saúde | `/health/live` (processo) e `/health/ready` (banco + fila) |

## 20. Limitações e interpretações

| Ponto | Situação |
|---|---|
| permissões do SQS | configuradas, mas o LocalStack gratuito não as aplica |
| ordem dos eventos | garantida por carteira dentro de um publicador; com vários, usar `walletVersion` |
| moedas | só as de 2 casas decimais |
| carteira inexistente | não vira operação gravada; responde `404` ou vai para a DLQ |
| campos desconhecidos no corpo | recusados com `400` |
| WIN com referência | interpretação adotada: quando informada, precisa ser uma BET da mesma rodada, e o WIN espera por ela como um reembolso |
| BET e LOSS com referência | recusados com `400` |
| aceite assíncrono | não existe: operações sem dependência são decididas e gravadas de uma vez; o worker também retomaria um `PENDING` gravado |
| `FAILED` | só ocorre ao retomar pendências; no caminho direto, um erro inesperado devolve `500` sem gravar nada, e a operação pode ser repetida |
| `/metrics` | público, na mesma porta da API |
