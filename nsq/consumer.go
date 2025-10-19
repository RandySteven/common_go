package nsq

import (
	"context"
	"fmt"
	"log"
)

// Consume retrieves a message from the specified topic by checking the context.
// It looks for a value associated with the topic in the provided context.
// If found, it returns the message as a string; otherwise, it returns an error.
// This method is typically used within consumer handlers to access received messages.
func (c *Client) Consume(ctx context.Context, topic string) (value string, err error) {
	if ctx.Value(topic) != nil {
		log.Println(`context value : `, ctx.Value(topic).(string))
		return ctx.Value(topic).(string), nil
	} else {
		log.Println(`context value : `, nil)
		return "", fmt.Errorf(`failed to consume the topic %s`, topic)
	}
}
