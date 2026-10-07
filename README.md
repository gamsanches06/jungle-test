# jungle-test

## O que o projeto faz

É um serviço que guarda o saldo de jogadores de jogos online. Empresas de jogos (chamadas de
**provedores**) avisam o serviço quando algo acontece com o dinheiro do jogador:

| Tipo | O que acontece com o saldo | Exemplo |
|---|---|---|
| `BET` (aposta) | diminui | o jogador aposta 25,00 |
| `WIN` (ganho) | aumenta | o jogador ganha 50,00 |
| `LOSS` (perda) | não muda | registra que a rodada acabou sem prêmio; o valor é sempre `0.00` |
| `REFUND` (reembolso) | aumenta | devolve uma aposta inteira |
| `ROLLBACK` (estorno) | faz o contrário da operação original | desfaz uma aposta, um ganho ou um reembolso |

O provedor pode avisar de dois jeitos: chamando a API HTTP ou mandando uma mensagem numa fila (SQS, o
serviço de filas da AWS). Os dois caminhos chegam no mesmo código e têm o mesmo resultado.

Cada movimentação vira uma linha num **ledger**, que é um livro-caixa: uma lista em que só se acrescentam
linhas, nunca se apaga nem se altera. Somando os créditos e subtraindo os débitos do ledger, chega-se ao
saldo da carteira.

As decisões técnicas estão no [ARCHITECTURE.md](ARCHITECTURE.md).

## O que você precisa ter instalado

- Docker com Compose (o comando `docker compose`). Ele sobe tudo: banco, login, fila e o serviço.
- Go 1.27.1, só se quiser rodar os testes ou as ferramentas fora do Docker.
- gcc, só para o teste com detector de concorrência (`go test -race`).
- `curl` e `jq`, só para as chamadas manuais.

## Como rodar

```bash
docker compose up --build -d
docker compose ps
```

O segundo comando mostra os containers. Espere todos ficarem `healthy` (saudáveis). O `migrate` aparece
como `exited`, porque ele só cria as tabelas do banco e termina.

O que fica rodando:

| Container | Para que serve | Porta |
|---|---|---|
| `postgres` | banco de dados PostgreSQL, onde fica tudo que importa | 5432 |
| `keycloak` | sistema de login que entrega os tokens de acesso | 8081 |
| `localstack` | imitação local da AWS, com as filas SQS | 4566 |
| `migrate` | cria as tabelas e termina | nenhuma |
| `app1`, `app2`, `app3` | três cópias do serviço, rodando ao mesmo tempo | 8080, 8082, 8083 |

As três cópias existem para mostrar que o serviço funciona certo mesmo quando vários processos mexem na
mesma carteira ao mesmo tempo.

Tudo é configurado sozinho na subida:

- o PostgreSQL cria o banco `jungle_test`, o dono `jungle` e o usuário da aplicação `jungle-app`
  (`deploy/postgres/01-roles.sh`);
- o Keycloak importa o realm `jungle` com os usuários de teste (`deploy/keycloak/realm-jungle.json`);
- o LocalStack cria as filas (`deploy/localstack/init-sqs.sh`);
- o `migrate` cria as tabelas.

Para desligar e apagar os dados:

```bash
docker compose down -v
```

## Variáveis de ambiente

O arquivo [.env.example](.env.example) tem todas as variáveis com valores locais de exemplo, sem segredos
reais. O `docker-compose.yml` já define os valores para os containers; o `.env.example` serve para rodar o
serviço fora do Docker. As principais:

| Variável | Padrão | Para quê |
|---|---|---|
| `APP_ROLES` | `api,consumer,outbox,pending` | partes ligadas na instância: API, leitor da fila, publicador de eventos, worker de pendências |
| `HTTP_ADDR` | `:8080` | porta da API |
| `DATABASE_URL` | obrigatória | conexão do usuário `jungle-app` |
| `OIDC_ISSUER` | obrigatória | emissor esperado no token (`http://localhost:8081/realms/jungle`) |
| `OIDC_JWKS_URL` | obrigatória | onde buscar as chaves que assinam os tokens |
| `OIDC_AUDIENCE` | `jungle-api` | para quem o token precisa ter sido emitido |
| `AWS_ENDPOINT_URL` | vazio | endereço do SQS; no LocalStack, `http://localhost:4566` |
| `SQS_INPUT_QUEUE`, `SQS_DLQ_QUEUE`, `SQS_EVENTS_QUEUE` | ver seção Filas | nomes das filas |
| `SQS_VISIBILITY_TIMEOUT` | `30s` | tempo que uma mensagem lida fica invisível para os outros |
| `SQS_MAX_RECEIVES` | `5` | quantas vezes uma mensagem pode falhar antes de ir para a DLQ |
| `OUTBOX_POLL_INTERVAL` | `250ms` | frequência do publicador de eventos |
| `REFERENCE_TTL` | `10m` | quanto tempo esperar uma operação de referência que ainda não chegou |
| `SHUTDOWN_TIMEOUT` | `25s` | prazo para desligar terminando o trabalho em andamento |
| `FAILPOINTS` | vazio | injeta falhas para testes (ver Várias instâncias e simulação de falhas) |

Se alguma variável estiver errada, o serviço não sobe e encerra com código 2.

## Filas

O LocalStack roda `deploy/localstack/init-sqs.sh` assim que fica pronto:

| Fila | Para quê |
|---|---|
| `wager-transactions.fifo` | entrada das operações enviadas pelos provedores |
| `wager-transactions-dlq.fifo` | mensagens que não puderam ser processadas (DLQ) |
| `wallet-events.fifo` | eventos publicados pelo serviço, como "saldo mudou" |
| `wallet-events-dlq.fifo` | DLQ de quem consome os eventos |

O script também cria as permissões de cada papel na fila. Para rodar de novo ou listar as filas:

```bash
docker compose exec localstack /etc/localstack/init/ready.d/init-sqs.sh
docker compose exec localstack awslocal sqs list-queues
```

## Banco e migrations

As tabelas são criadas por migrations versionadas na pasta `migrations/`. Elas rodam com o dono do banco
(`jungle`), nunca com o usuário da aplicação.

| Versão | Cria |
|---|---|
| 000001 | carteiras, operações e ledger, com as regras de unicidade |
| 000002 | inbox e outbox |
| 000003 | travas que impedem alterar o ledger e permissões do `jungle-app` |

Pelo Docker:

```bash
docker compose run --rm migrate                                   # aplica todas
docker compose run --rm --entrypoint /migrate migrate down 1      # desfaz a última
docker compose run --rm --entrypoint /migrate migrate version     # mostra a versão atual
```

Pelo Go, fora do Docker:

```bash
export MIGRATE_DATABASE_URL=postgres://jungle:jungle-1234@localhost:5432/jungle_test?sslmode=disable
go run ./cmd/migrate up          # aplica todas
go run ./cmd/migrate down 1      # desfaz a última
go run ./cmd/migrate down all    # desfaz todas
go run ./cmd/migrate version
```

## Rodar o serviço fora do Docker

```bash
docker compose up -d --wait postgres keycloak localstack migrate
set -a; . ./.env.example; set +a
go run ./cmd/wagering            # API em http://localhost:8090
```

## Como testar sem escrever código

**Opção 1: o validador automático.** Ele faz chamadas reais às três cópias e à fila, e imprime ✔ ou ✘ para
cada uma das 63 verificações:

```bash
go run ./cmd/e2e
```

**Opção 2: o Postman.** A pasta `postman/` tem uma coleção pronta:

1. No Postman, clique em **Import** e escolha `jungle-test.postman_collection.json` e
   `jungle-test-localhost.postman_environment.json`.
2. Selecione o environment **jungle-test localhost** no canto superior direito.
3. Clique na coleção, depois em **Run** e em **Run jungle-test**.

A coleção pega os tokens sozinha e passa os ids de uma chamada para a outra. Se o serviço estiver em outra
máquina, troque `localhost` pelo endereço dela nas variáveis `api1`, `api2`, `api3` e `keycloak` do
environment.

## Como fazer chamadas na mão

Toda chamada precisa de um token, que é uma espécie de crachá temporário (dura 5 minutos). Quem dá o
crachá é o Keycloak.

Usuários de teste que já vêm configurados:

| Usuário (`client_id`) | Pode fazer |
|---|---|
| `wallet-service` | criar carteiras, ver saldo e ledger, conferir o saldo (reconciliação) |
| `provider-a` | enviar apostas e consultar as próprias operações |
| `provider-b` | o mesmo, mas não enxerga nada do `provider-a` |
| `provider-a-shortlived` | igual ao `provider-a`, com token de 3 segundos, para testar token vencido |
| `no-role-client` | entra, mas não pode fazer nada (testa o `403`) |

A senha (`client_secret`) é sempre o nome do usuário seguido de `-secret`.

Rotas principais:

| Rota | O que faz | Quem pode |
|---|---|---|
| `POST /wallets` | cria uma carteira | `wallet-service` |
| `GET /wallets/{id}` | mostra saldo e versão | `wallet-service` |
| `GET /wallets/{id}/ledger` | lista as linhas do livro-caixa, em páginas | `wallet-service` |
| `POST /wallets/{id}/reconciliation` | soma o ledger e compara com o saldo guardado | `wallet-service` |
| `POST /wagering/transactions` | envia uma operação (BET, WIN…) | provedores |
| `GET /wagering/transactions/{id}` | consulta uma operação | provedor dono ou `wallet-service` |
| `GET /providers/{provedor}/wagering/transactions/{idExterno}` | consulta pelo id que o provedor usa | provedor dono ou `wallet-service` |
| `GET /health/live`, `GET /health/ready` | diz se o serviço está vivo e se banco e fila respondem | qualquer um |

Ao enviar uma operação, é obrigatório o header `Idempotency-Key`. Ele é o "número de protocolo" da
operação: se a mesma chave chegar duas vezes, o serviço não cobra de novo e devolve a resposta da
primeira vez, com `"idempotentReplay": true`.

Um exemplo do começo ao fim:

```bash
token() { curl -s -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret \
  http://localhost:8081/realms/jungle/protocol/openid-connect/token | jq -r .access_token; }
INTERNAL=$(token wallet-service)
PROVIDER=$(token provider-a)
PLAYER=$(cat /proc/sys/kernel/random/uuid)

# cria uma carteira com 1000,00
WALLET=$(curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $INTERNAL" \
  -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}" | jq -r .id)

# aposta 25,00 na cópia 2; rode duas vezes e a segunda volta com "idempotentReplay": true
curl -s -X POST localhost:8082/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:bet-1' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-1\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# devolve a aposta na cópia 3
curl -s -X POST localhost:8083/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:refund-1' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"refund-1\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"REFUND\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"bet-1\"}"

# consultas
curl -s localhost:8080/wallets/$WALLET -H "Authorization: Bearer $INTERNAL" | jq
curl -s "localhost:8080/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL" | jq
curl -s localhost:8080/providers/provider-a/wagering/transactions/bet-1 -H "Authorization: Bearer $PROVIDER" | jq
curl -s -X POST localhost:8080/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL" | jq
```

A mesma aposta também pode chegar pela fila. Se ela já foi feita por HTTP, o serviço reconhece a chave e
não cobra de novo:

```bash
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-1 \
  --message-body "{\"messageId\":\"msg-1\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-09-08T12:00:00.000Z\",
    \"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-1\",\"idempotencyKey\":\"provider-a:bet-1\",
    \"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
    \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}}"
```

Para ver os eventos publicados:

```bash
docker compose exec localstack awslocal sqs receive-message --max-number-of-messages 10 \
  --queue-url http://localhost:4566/000000000000/wallet-events.fifo
```

Os códigos de resposta e de erro estão no [ARCHITECTURE.md §11 e §12](ARCHITECTURE.md#11-códigos-de-falha).

## Como rodar os testes

Testes rápidos, sem banco nem fila:

```bash
go test ./...
go test -race ./...
go vet ./...
gofmt -l .          # não imprimir nada significa que o código está formatado
```

Testes de integração, com banco, login e fila de verdade. Eles usam a tag `integration` e levam uns
2 minutos:

```bash
docker compose up -d --wait postgres keycloak localstack
go test -tags integration -count=1 -timeout 20m ./test/integration/
go test -race -tags integration -count=1 -timeout 20m ./test/integration/
go vet -tags integration ./test/...
```

Cada teste cria um banco e filas só para ele, então dá para rodar com tudo no ar. Para rodar um teste
só, acrescente `-v -run NomeDoTeste`.

Os testes que cobrem os cenários obrigatórios do enunciado:

| Cenário | Teste |
|---|---|
| a mesma aposta 50 vezes em paralelo gera um único débito | `TestSameBetFiftyTimesInParallel` |
| duas apostas de 80,00 sobre 100,00: uma passa, outra é recusada | `TestTwoConcurrentBetsOfEightyOnHundred` |
| carteiras diferentes não esperam umas pelas outras | `TestIndependentWalletsProgressInParallel` |
| os cenários acima com três processos separados | `TestThreeIndependentInstances` |
| leitor da fila morre depois de gravar e antes de apagar a mensagem | `TestConsumerCrashAfterCommitBeforeDelete` |
| dois publicadores disputando os mesmos eventos | `TestOutboxCompetingPublishers` |
| publicador morre antes ou depois de publicar | `TestOutboxRecoversCrashBetweenClaimAndPublish`, `TestOutboxRecoversCrashBetweenPublishAndConfirm` |
| reembolso chega antes da aposta | `TestPendingReferenceResolution` |
| todos os processos reiniciam | `TestFullRestartPreservesIdempotencyPendingAndBalances` |
| a mesma operação por HTTP e pela fila | `TestSQSConsumerDeduplicatesAndSharesIdempotencyWithHTTP` |
| login real, token ausente, inválido ou vencido, isolamento entre provedores | `TestAuthenticationWithKeycloak`, `TestAuthorizationAndProviderIsolation` |
| migrations, regras do banco e ledger imutável | `TestMigrationsUpDownUp`, `TestSchemaEnforcesFinancialInvariants` |
| banco ou fila fora do ar | `TestPostgresTemporarilyUnavailable`, `TestSQSTemporarilyUnavailable` |
| ligar e desligar o serviço | `TestGraphIsComplete`, `TestFxLifecycleReleasesResources` |

Todos os testes de integração terminam conferindo que o saldo guardado é igual aos créditos menos os
débitos do ledger.

## Várias instâncias e simulação de falhas

O `docker compose up` já sobe três cópias independentes, cada uma com sua memória e suas conexões. Os testes
de integração sobem os próprios processos, sem depender do compose.

Para simular falhas com a stack no ar:

| Falha | Comando | O que deve acontecer |
|---|---|---|
| banco fora do ar | `docker compose stop postgres`, depois `start` | a API responde `503`; `/health/ready` mostra o banco como `down`; ao voltar, repetir a mesma chave processa uma vez |
| fila fora do ar | `docker compose stop localstack`, depois `start` | a API continua funcionando; os eventos esperam e saem quando a fila volta |
| uma cópia morre de repente | `docker compose kill -s SIGKILL app1`, depois `start app1` | as outras cópias seguem; a reconciliação continua consistente |
| desligamento normal | `docker compose stop app2` | nos logs: `http server draining`, `sqs consumer stopped`, `pending resolver stopped`, `outbox relay stopped`, `postgres pool closed`, nessa ordem |
| todas as cópias reiniciam | `docker compose restart app1 app2 app3` | reenviar uma operação já feita devolve `idempotentReplay: true` com o saldo original |

A variável `FAILPOINTS` faz o serviço morrer ou falhar num ponto exato. Pontos disponíveis:
`process.before_commit`, `process.after_commit`, `consumer.after_commit_before_delete`,
`outbox.after_claim_before_publish`, `outbox.after_publish_before_ack`, `publisher.send`,
`consumer.process` e `pending.before_resolve`. As ações são `exit` (morre) e `error` (falha).

Exemplo: o leitor da fila morre depois de gravar a operação e antes de apagar a mensagem.

```bash
docker compose stop app1 app2 app3
docker compose run --rm -d --no-deps --name crashy \
  -e APP_ROLES=consumer -e INSTANCE_ID=crashy -e FAILPOINTS=consumer.after_commit_before_delete=exit app1
sleep 5
# envie uma mensagem como no exemplo de SQS acima, com messageId "crash-1" e externalTransactionId novo
docker wait crashy                 # o processo morre logo depois de gravar
docker compose start app1 app2 app3
sleep 35                           # a mensagem volta para a fila depois de 30 segundos
docker compose exec -T postgres psql -U jungle -d jungle_test -c \
  "select message_id, deliveries, outcome from inbox_messages where message_id = 'crash-1'"
```

O resultado esperado é `deliveries = 2`, com uma única operação e uma única linha no ledger.

## Teste de carga (opcional)

```bash
go run ./cmd/loadtest -duration 30s -concurrency 32 -wallets 50
```

Mostra requisições por segundo, tempos de resposta (p50, p95, p99), erros, conflitos, atraso dos eventos e
confere o saldo de todas as carteiras usadas. Numa VM de 2 CPUs com tudo rodando, uma execução de 20
segundos fez 447 requisições por segundo, com p99 de 273 ms e nenhum erro.

## Onde está cada coisa no código

| Pasta | Conteúdo |
|---|---|
| `internal/domain` | as regras de negócio: dinheiro, carteira, operações. Não sabe nada de HTTP nem de banco |
| `internal/application` | o passo a passo de cada operação, por exemplo "receber aposta" |
| `internal/infra/postgres` | os comandos SQL que leem e gravam no banco |
| `internal/infra/sqsx` | quem lê a fila de entrada e quem publica os eventos |
| `internal/transport/httpapi` | as rotas HTTP |
| `internal/auth` | a checagem do token |
| `internal/worker` | tarefas que rodam em segundo plano |
| `internal/app` | onde as peças são montadas (com a biblioteca Uber Fx) |
| `migrations` | os comandos que criam as tabelas do banco |
| `deploy` | a configuração inicial do PostgreSQL, do Keycloak e do LocalStack |
| `postman` | coleção e environment para o Postman |
| `test/integration` | os testes com banco, login e fila reais |
| `cmd` | os programas: o serviço, as migrations, o validador e o teste de carga |
