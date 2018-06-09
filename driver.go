package brokerutil

// Flag should reflect the ability of a driver to be used
// in concurrent environments
type Flag int

const (

	// ConcurrentExecution is the Type value used to
	// indicate that the pub sub driver supports concurrent
	// use
	ConcurrentExecution Flag = iota

	// BlockingExecution is the Type value used to
	// indicate that the pub sub driver does not support
	// concurrent use
	BlockingExecution Flag = iota
)

// PubSubDriver is the simplest pub sub driver
// requirement to be used initially
type PubSubDriver interface {

	// GetDriverFlags should reflect the ability of the driver to
	// be used in concurrent environments such as multiple
	// goroutines pub'n'subbing concurrently
	GetDriverFlags() []Flag

	// CloseStream is called by the driver consumer when
	// the pub-sub stream is to be closed
	CloseStream() error

	// OpenStream is called by the driver consumer when
	// the pub-sub stream is to be opened
	OpenStream() error
}

// BlockingPubSubDriver is the implementation
// contract for a pub sub driver which does not support concurrent
// use
//
// NotifyMessageRecieve() and NotifyMessageTest() can both be blocking,
// but no message will be sent / published during that block to follow
// the unsupported concurrent use restriction.
type BlockingPubSubDriver interface {
	PubSubDriver

	// ReceiveMessage is called by the driver consumer to
	// recieve a message
	//
	// ReceiveMessage can be blocking
	ReceiveMessage() (interface{}, error)

	// PublishMessage is called by the driver consumer to
	// publish a message.
	PublishMessage(interface{}) error
}

// ConcurrentPubSubDriver is the implementation
// contract for a pub sub driver which does support concurrent
// use.
type ConcurrentPubSubDriver interface {
	PubSubDriver

	// GetMessageWriterChannel is called by the driver consumer
	// to get the writer channel of the driver.
	//
	// Messages written to the channel are to be sent to the
	// message broker by the driver.
	GetMessageWriterChannel() (chan<- interface{}, error)

	// GetMessageReaderChannel is called by the driver consumer
	// to get the reader channel of the driver.
	//
	// Recieved messages from the message broker are to be written
	// to this channel by the driver.
	GetMessageReaderChannel() (<-chan interface{}, error)
}
