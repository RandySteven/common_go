package nsq

import (
	"context"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

type (
	// ConsumerFunc defines the signature for a consumer function that processes messages
	// from a specific topic. It receives a context and topic name, and returns an error.
	ConsumerFunc func(ctx context.Context, topic string) (err error)

	// NsqEvent represents a message event in NSQ with topic and message content.
	NsqEvent struct {
		Topic   string // The topic name where the message will be published
		Message []byte // The actual message content as bytes
	}

	// NSQ defines the interface for NSQ operations including publishing, consuming, and registering consumers.
	NSQ interface {
		// Publish sends a message to the specified topic
		Publish(ctx context.Context, event *NsqEvent) (err error)
		// Consume retrieves a message from the specified topic
		Consume(ctx context.Context, topic string) (value string, err error)
		// RegisterConsumer sets up a consumer function for a specific topic
		RegisterConsumer(topic string, cf ConsumerFunc) (err error)
	}

	// Client represents an NSQ client that handles publishing and consuming messages.
	Client struct {
		Pub     *nsq.Producer // NSQ producer for publishing messages
		Config  *nsq.Config   // NSQ configuration settings
		Lookupd string        // NSQ lookupd address for service discovery
	}

	// NSQConfig holds configuration parameters for connecting to NSQ.
	NSQConfig struct {
		Host     string // NSQ host address
		DTCPPort string // TCP port for NSQ daemon
		HTTPPort string // HTTP port for NSQ lookupd
	}
)

// RegisterConsumer creates and registers a consumer for the specified topic.
// It sets up a handler that processes incoming messages using the provided ConsumerFunc.
// The consumer will automatically connect to NSQ lookupd and start processing messages.
// Returns an error if the consumer creation or connection fails.
func (c *Client) RegisterConsumer(topic string, cf ConsumerFunc) (err error) {
	consumer, err := nsq.NewConsumer(topic, "channel", c.Config)
	if err != nil {
		return err
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		body := string(message.Body)
		ctx := context.WithValue(context.Background(), topic, body)
		ctx, cancel := context.WithTimeout(ctx, time.Second*30)
		defer cancel()

		if err := func() error {
			cf(ctx, topic)
			return nil
		}(); err != nil {
			log.Println("Error in handlerFunc:", err)
			message.Requeue(-1)
			return err
		}

		return nil
	}))

	if err = consumer.ConnectToNSQLookupd(c.Lookupd); err != nil {
		return err
	}
	return nil
}

var _ NSQ = &Client{}

// NewNSQClient creates a new NSQ client instance with the provided configuration.
// It initializes both the producer and lookupd connection settings.
// Returns an NSQ interface implementation or an error if initialization fails.
func NewNSQClient(config *NSQConfig) (result NSQ, err error) {
	nsqConfig := nsq.NewConfig()

	addr := fmt.Sprintf("%s:%s", config.Host, config.DTCPPort)
	producer, err := nsq.NewProducer(addr, nsqConfig)
	if err != nil {
		return nil, err
	}

	return &Client{
		Pub:     producer,
		Config:  nsqConfig,
		Lookupd: fmt.Sprintf("%s:%s", config.Host, config.HTTPPort),
	}, nil
}
