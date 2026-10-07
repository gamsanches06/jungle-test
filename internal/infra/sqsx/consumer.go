package sqsx

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/config"
	"github.com/gamsanches06/jungle-test/internal/observability"
	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

// Consumer reads wager-transactions.fifo and runs the same use case as HTTP.
//
//   - A message is deleted only after the SQL transaction that recorded its
//     inbox entry, the financial changes and the outbox events committed.
//   - Business rejections are committed results, so the message is deleted.
//   - Invalid messages and permanent input errors are forwarded to the DLQ
//     and deleted.
//   - Transient failures keep the message and extend its visibility with
//     exponential backoff; after SQS_MAX_RECEIVES receptions the queue's
//     redrive policy moves it to the DLQ.
//   - Messages of the same MessageGroupId (wallet) are processed in order;
//     different groups are processed in parallel.
type Consumer struct {
	cfg     config.Config
	clients *Clients
	queues  *Queues
	process *application.ProcessService
	metrics *observability.Metrics
	log     *slog.Logger

	pollCtx    context.Context
	stopPoll   context.CancelFunc
	workCtx    context.Context
	cancelWork context.CancelFunc
	wg         sync.WaitGroup
	done       chan struct{}
}

// NewConsumer builds the consumer.
func NewConsumer(cfg config.Config, clients *Clients, queues *Queues, process *application.ProcessService, metrics *observability.Metrics, log *slog.Logger) *Consumer {
	return &Consumer{cfg: cfg, clients: clients, queues: queues, process: process, metrics: metrics,
		log: log.With(slog.String("component", "sqs-consumer")), done: make(chan struct{})}
}

// Start launches the pollers.
func (c *Consumer) Start() {
	c.pollCtx, c.stopPoll = context.WithCancel(context.Background())
	c.workCtx, c.cancelWork = context.WithCancel(context.Background())
	for i := 0; i < c.cfg.SQSPollers; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.poll()
		}()
	}
	go func() {
		c.wg.Wait()
		close(c.done)
	}()
	c.log.Info("sqs consumer started", slog.Int("pollers", c.cfg.SQSPollers), slog.String("queue", c.queues.Input()))
}

// Stop stops receiving, lets in-flight messages finish until ctx expires,
// then cancels them; unfinished messages get their visibility released so
// another instance receives them immediately.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.stopPoll == nil {
		return nil
	}
	c.stopPoll()
	select {
	case <-c.done:
		c.log.Info("sqs consumer stopped")
		return nil
	case <-ctx.Done():
		c.cancelWork()
		<-c.done
		c.log.Warn("sqs consumer stopped after cancelling in-flight work")
		return nil
	}
}

// Done is closed when every poller has terminated.
func (c *Consumer) Done() <-chan struct{} { return c.done }

func (c *Consumer) poll() {
	backoff := 200 * time.Millisecond
	for c.pollCtx.Err() == nil {
		out, err := c.clients.Consumer.ReceiveMessage(c.pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.queues.Input()),
			MaxNumberOfMessages:         c.cfg.SQSMaxMessages,
			WaitTimeSeconds:             int32(c.cfg.SQSWaitTime / time.Second),
			VisibilityTimeout:           int32(c.cfg.SQSVisibility / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			if c.pollCtx.Err() != nil {
				return
			}
			c.log.Warn("sqs receive failed", slog.String("error", err.Error()))
			c.metrics.Retry("sqs-receive")
			select {
			case <-c.pollCtx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = 200 * time.Millisecond
		c.handleBatch(out.Messages)
	}
}

// handleBatch keeps FIFO order inside each message group.
func (c *Consumer) handleBatch(msgs []types.Message) {
	if len(msgs) == 0 {
		return
	}
	var order []string
	groups := map[string][]types.Message{}
	for _, m := range msgs {
		g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], m)
	}
	var wg sync.WaitGroup
	for _, g := range order {
		group := groups[g]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, m := range group {
				if c.pollCtx.Err() != nil || c.workCtx.Err() != nil {
					c.release(group[i:])
					return
				}
				if !c.handle(m) {
					// Later messages of the group go back to the queue so they run after this one.
					c.release(group[i+1:])
					return
				}
			}
		}()
	}
	wg.Wait()
}

// handle processes one message; it returns false when the message was left
// in the queue for a retry.
func (c *Consumer) handle(m types.Message) bool {
	body := aws.ToString(m.Body)
	receiveCount, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	ctx := observability.WithFields(c.workCtx, slog.String("sqsMessageId", aws.ToString(m.MessageId)))

	msg, err := ParseMessage([]byte(body))
	if err != nil {
		c.log.WarnContext(ctx, "invalid sqs message", slog.String("error", err.Error()))
		c.toDLQ(ctx, m, "INVALID_MESSAGE", err)
		return true
	}
	correlation := msg.MessageID
	if a, ok := m.MessageAttributes["correlationId"]; ok && aws.ToString(a.StringValue) != "" {
		correlation = aws.ToString(a.StringValue)
	}
	ctx = observability.WithFields(ctx,
		slog.String(observability.FieldMessageID, msg.MessageID),
		slog.String(observability.FieldCorrelationID, correlation),
		slog.String(observability.FieldProviderID, msg.Request.ProviderID()),
		slog.String(observability.FieldWalletID, msg.Request.WalletID().String()))

	pctx, cancel := context.WithTimeout(ctx, c.cfg.SQSProcessTimeout)
	defer cancel()
	var res application.ProcessResult
	err = failpoint.Inject(failpoint.ConsumerProcessTransiently)
	if err != nil {
		err = application.Transient(err)
	} else {
		res, err = c.process.Process(pctx, application.ProcessCommand{
			Request: msg.Request, IdempotencyKey: msg.IdempotencyKey, CorrelationID: correlation,
			Source: application.SourceSQS,
			Inbox: &application.InboxMessage{
				Consumer: c.cfg.ConsumerName, MessageID: msg.MessageID,
				PayloadHash: msg.InboxHash(), ReceivedAt: time.Now().UTC().Truncate(time.Microsecond),
			},
		})
	}
	switch {
	case err == nil:
		if res.InboxDuplicate {
			c.metrics.InboxDuplicates.Inc()
			c.log.InfoContext(ctx, "duplicate sqs delivery deduplicated by inbox",
				slog.String(observability.FieldTransactionID, res.Transaction.ID().String()),
				slog.Int("receiveCount", receiveCount))
		}
		// A crash here leaves the work committed and the message in the queue;
		// the inbox deduplicates the redelivery.
		_ = failpoint.Inject(failpoint.ConsumerAfterCommit)
		c.delete(ctx, m)
		c.metrics.SQSMessages.WithLabelValues(string(res.Transaction.Status())).Inc()
		return true
	case application.IsPermanentInput(err):
		c.toDLQ(ctx, m, string(application.CodeOf(err)), err)
		return true
	default:
		if errors.Is(err, context.Canceled) && c.workCtx.Err() != nil {
			c.release([]types.Message{m})
			return false
		}
		if receiveCount >= c.cfg.SQSMaxReceives {
			c.metrics.DLQ.WithLabelValues("max_receives").Inc()
		}
		c.metrics.Retry("sqs")
		c.log.WarnContext(ctx, "sqs message processing failed, will retry",
			slog.String("error", err.Error()), slog.Int("receiveCount", receiveCount))
		c.backoff(ctx, m, receiveCount)
		return false
	}
}

func (c *Consumer) backoff(ctx context.Context, m types.Message, receiveCount int) {
	d := c.cfg.SQSRetryBase
	for i := 1; i < receiveCount; i++ {
		d *= 2
		if d >= c.cfg.SQSRetryMax {
			d = c.cfg.SQSRetryMax
			break
		}
	}
	c.changeVisibility(ctx, m, int32(d/time.Second))
}

func (c *Consumer) release(msgs []types.Message) {
	for _, m := range msgs {
		c.changeVisibility(context.Background(), m, 0)
	}
}

func (c *Consumer) changeVisibility(ctx context.Context, m types.Message, seconds int32) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := c.clients.Consumer.ChangeMessageVisibility(cctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queues.Input()), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: seconds,
	})
	if err != nil {
		c.log.WarnContext(ctx, "change visibility failed", slog.String("error", err.Error()))
	}
}

func (c *Consumer) delete(ctx context.Context, m types.Message) {
	for attempt := 1; attempt <= 3; attempt++ {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_, err := c.clients.Consumer.DeleteMessage(dctx, &sqs.DeleteMessageInput{
			QueueUrl: aws.String(c.queues.Input()), ReceiptHandle: m.ReceiptHandle,
		})
		cancel()
		if err == nil {
			return
		}
		c.log.WarnContext(ctx, "delete message failed; redelivery will be deduplicated",
			slog.String("error", err.Error()), slog.Int("attempt", attempt))
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
}

// toDLQ forwards a message that can never succeed to the DLQ with the reason
// as message attributes, then deletes it from the input queue.
func (c *Consumer) toDLQ(ctx context.Context, m types.Message, reason string, cause error) {
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	detail := cause.Error()
	if len(detail) > 256 {
		detail = detail[:256]
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := c.clients.Consumer.SendMessage(sctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.queues.DLQ()),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureCode":        {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"failureDetail":      {DataType: aws.String("String"), StringValue: aws.String(detail)},
			"sourceSqsMessageId": {DataType: aws.String("String"), StringValue: m.MessageId},
		},
	})
	if err != nil {
		c.log.ErrorContext(ctx, "forward to dlq failed; message kept for retry", slog.String("error", err.Error()))
		c.backoff(ctx, m, 1)
		return
	}
	c.metrics.DLQ.WithLabelValues(reason).Inc()
	c.log.WarnContext(ctx, "message forwarded to dlq", slog.String("failureCode", reason), slog.String("error", detail))
	c.delete(ctx, m)
}
