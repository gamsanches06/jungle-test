// Package sqsx integrates with AWS SQS (LocalStack locally): queue
// resolution, the input consumer and the event publisher.
package sqsx

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gamsanches06/jungle-test/internal/config"
)

// Clients holds one SQS client per broker role, each with its own
// credentials, so broker policies can grant least privilege per role.
type Clients struct {
	Consumer  *sqs.Client
	Publisher *sqs.Client
}

// NewClients builds the SQS clients.
func NewClients(cfg config.Config) (*Clients, error) {
	consumer, err := newClient(cfg, cfg.ConsumerAccessKey, cfg.ConsumerSecretKey)
	if err != nil {
		return nil, err
	}
	publisher, err := newClient(cfg, cfg.PublisherAccessKey, cfg.PublisherSecretKey)
	if err != nil {
		return nil, err
	}
	return &Clients{Consumer: consumer, Publisher: publisher}, nil
}

func newClient(cfg config.Config, accessKey, secretKey string) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.AWSRegion)}
	if accessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	}), nil
}

// Queues holds the resolved queue URLs.
type Queues struct {
	mu     sync.RWMutex
	input  string
	dlq    string
	events string
}

func (q *Queues) Input() string  { q.mu.RLock(); defer q.mu.RUnlock(); return q.input }
func (q *Queues) DLQ() string    { q.mu.RLock(); defer q.mu.RUnlock(); return q.dlq }
func (q *Queues) Events() string { q.mu.RLock(); defer q.mu.RUnlock(); return q.events }

// Resolve looks up the queue URLs, retrying until ctx expires. It validates
// that the broker is reachable and the queues are provisioned.
func (q *Queues) Resolve(ctx context.Context, c *Clients, cfg config.Config, log *slog.Logger) error {
	get := func(client *sqs.Client, name string) (string, error) {
		for {
			out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
			if err == nil {
				return aws.ToString(out.QueueUrl), nil
			}
			log.WarnContext(ctx, "sqs queue not available", slog.String("queue", name), slog.String("error", err.Error()))
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("resolve queue %s: %w", name, err)
			case <-time.After(time.Second):
			}
		}
	}
	input, err := get(c.Consumer, cfg.InputQueueName)
	if err != nil {
		return err
	}
	dlq, err := get(c.Consumer, cfg.DLQName)
	if err != nil {
		return err
	}
	events, err := get(c.Publisher, cfg.EventsQueueName)
	if err != nil {
		return err
	}
	q.mu.Lock()
	q.input, q.dlq, q.events = input, dlq, events
	q.mu.Unlock()
	return nil
}

// Ping checks the broker for readiness.
func Ping(ctx context.Context, c *Clients, q *Queues) error {
	url := q.Input()
	if url == "" {
		return fmt.Errorf("queues not resolved")
	}
	_, err := c.Consumer.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}
