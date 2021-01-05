package metrics

type ChannelMetrics struct {
	// The length of the channel when popping the message from the channel
	Length Counter

	// The time between pushing and popping the message to/from the channel.
	WaitTime Guage
}
