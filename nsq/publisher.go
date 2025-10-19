package nsq

import "context"

// Publish sends a message to the specified NSQ topic.
// It takes an NsqEvent containing the topic name and message content,
// and publishes it using the underlying NSQ producer.
// Returns an error if the publish operation fails.
func (c *Client) Publish(ctx context.Context, event *NsqEvent) (err error) {
	return c.Pub.Publish(event.Topic, event.Message)
}
