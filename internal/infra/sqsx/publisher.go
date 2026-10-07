package sqsx

import (
	"context"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

// OutgoingEvent is an outbox record ready to be published.
type OutgoingEvent struct {
	EventID   string
	EventType string
	GroupKey  string
	Payload   string
}

// EventPublisher publishes integration events to wallet-events.fifo.
//
// Routing contract: MessageGroupId is the wallet id (events of one wallet
// keep their relative order inside one publisher) and MessageDeduplicationId
// is the eventId, so a republication within the SQS deduplication window is
// dropped by the broker; consumers must still deduplicate by eventId.
type EventPublisher struct {
	clients *Clients
	queues  *Queues
}

// NewEventPublisher builds the publisher.
func NewEventPublisher(c *Clients, q *Queues) *EventPublisher {
	return &EventPublisher{clients: c, queues: q}
}

// MaxBatch is the SQS limit of entries per SendMessageBatch.
const MaxBatch = 10

// PublishBatch sends up to MaxBatch events in one call. Order inside a
// message group is the order of the slice. It returns the per-event errors
// of entries the broker rejected; err is set when the whole call failed.
func (p *EventPublisher) PublishBatch(ctx context.Context, evs []OutgoingEvent) (map[string]error, error) {
	if len(evs) > MaxBatch {
		return nil, errors.New("batch too large")
	}
	if err := failpoint.Inject(failpoint.PublisherSend); err != nil {
		return nil, err
	}
	entries := make([]types.SendMessageBatchRequestEntry, len(evs))
	for i, e := range evs {
		entries[i] = types.SendMessageBatchRequestEntry{
			Id:                     aws.String(strconv.Itoa(i)),
			MessageBody:            aws.String(e.Payload),
			MessageGroupId:         aws.String(e.GroupKey),
			MessageDeduplicationId: aws.String(e.EventID),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"eventId":   {DataType: aws.String("String"), StringValue: aws.String(e.EventID)},
				"eventType": {DataType: aws.String("String"), StringValue: aws.String(e.EventType)},
			},
		}
	}
	out, err := p.clients.Publisher.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(p.queues.Events()), Entries: entries,
	})
	if err != nil {
		return nil, err
	}
	failed := map[string]error{}
	for _, f := range out.Failed {
		i, _ := strconv.Atoi(aws.ToString(f.Id))
		if i >= 0 && i < len(evs) {
			failed[evs[i].EventID] = errors.New(aws.ToString(f.Code) + ": " + aws.ToString(f.Message))
		}
	}
	return failed, nil
}
