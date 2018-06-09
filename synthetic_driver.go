package brokerutil

import (
	"reflect"
	"sync"
)

var (
	defaultSyntheticDriverOptions = &syntheticDriverOptions{
		UseSyntheticMessageWithSource: false,
		UseSyntheticMessageWithTarget: false,
	}
)

type metaDriverWrapper struct {
	executionFlag Flag
	driver        PubSubDriver
	out           chan interface{}
}

type syntheticDriverOptions struct {
	UseSyntheticMessageWithSource bool
	UseSyntheticMessageWithTarget bool
}

type syntheticMessageWithSource struct {
	source  PubSubDriver
	message interface{}
}

type syntheticMessageWithTarget struct {
	target  PubSubDriver
	message interface{}
}

// syntheticDriver is a ConcurrentPubSubDriverScaffold compliant
// pub sub driver which merges other pub sub drivers of the same type
// into one
type syntheticDriver struct {
	options *syntheticDriverOptions
	drivers []metaDriverWrapper
	out     chan interface{}
	in      chan interface{}
	sigTerm chan int
	sigMsg  chan string
}

// newSyntheticDriver creates a new driver merger which merges multiple drivers
// into one to be used as PubSub driver.
//
// The first driver is used to publish messages
func newSyntheticDriver(options *syntheticDriverOptions, drivers ...PubSubDriver) (*syntheticDriver, error) {

	// set executionFlag, check all drivers on compliance
	var metaDriverSlice = make([]metaDriverWrapper, 0)
	for _, d := range drivers {

		if containsFlag(d.GetDriverFlags(), BlockingExecution) {
			metaDriverSlice = append(metaDriverSlice, metaDriverWrapper{
				executionFlag: BlockingExecution,
				driver:        d,
				out:           make(chan interface{}, 1),
			})

		} else if containsFlag(d.GetDriverFlags(), ConcurrentExecution) {
			metaDriverSlice = append(metaDriverSlice, metaDriverWrapper{
				executionFlag: ConcurrentExecution,
				driver:        d,
				out:           make(chan interface{}, 1),
			})
		} else {
			return nil, ErrMissingExecutionFlag
		}
	}

	return &syntheticDriver{
		drivers: metaDriverSlice,
		options: options,
		out:     make(chan interface{}, 1),
		in:      make(chan interface{}, 1),
		sigTerm: make(chan int, 1),
		sigMsg:  make(chan string, 1),
	}, nil

}

func (p *syntheticDriver) getOptions() *syntheticDriverOptions {
	if p.options == nil {
		return defaultSyntheticDriverOptions
	}

	return p.options
}

func (p *syntheticDriver) encodeMessage(msg interface{}, d PubSubDriver) interface{} {

	if p.getOptions().UseSyntheticMessageWithSource {
		return syntheticMessageWithSource{
			message: msg,
			source:  d,
		}
	}

	return msg
}

func (p *syntheticDriver) decodeMessage(msg interface{}) (interface{}, PubSubDriver) {

	if !p.getOptions().UseSyntheticMessageWithTarget {
		return msg, nil
	}

	message, ok := msg.(syntheticMessageWithTarget)
	if !ok || message.target == nil {
		return msg, nil
	}

	return message.message, message.target
}

// GetDriverFlags returns the drivers flags to signal the type of execution
func (p *syntheticDriver) GetDriverFlags() []Flag {
	return []Flag{ConcurrentExecution}
}

// OpenStream opens each drivers streams and the relays those streams
// onto the mergers own streams
func (p *syntheticDriver) OpenStream() error {

	for _, d := range p.drivers {

		if err := d.driver.OpenStream(); err != nil {
			return err
		}

		var s = &sync.WaitGroup{}

		if d.executionFlag == BlockingExecution {

			driver, ok := d.driver.(BlockingPubSubDriver)
			if !ok {
				return ErrBlockingDriverCast
			}

			s.Add(1)

			go func() {

				receiveFromApplication := d.out

				s.Done()

				for {
					select {

					case <-p.sigTerm:
						return

					case msg := <-receiveFromApplication:
						driver.PublishMessage(msg)

					default:
						msg, _ := driver.ReceiveMessage()

						if msg != nil {
							p.in <- p.encodeMessage(msg, d.driver)
						}
					}
				}

			}()

		} else if d.executionFlag == ConcurrentExecution {

			driver, ok := d.driver.(ConcurrentPubSubDriver)
			if !ok {
				return ErrConcurrentDriverCast
			}

			s.Add(1)

			go func() {

				receiveFromApplication := d.out

				s.Done()

				transmitToDriver, _ := driver.GetMessageWriterChannel()

				for {
					select {
					case <-p.sigTerm:
						return
					case msg := <-receiveFromApplication:
						transmitToDriver <- msg
					}
				}
			}()

			go func() {

				receiveFromDriver, _ := driver.GetMessageReaderChannel()

				for {
					select {
					case <-p.sigTerm:
						return
					case msg := <-receiveFromDriver:
						p.in <- p.encodeMessage(msg, d.driver)
					}
				}
			}()
		}

		s.Wait()
	}

	go func() {

		first := p.drivers[0].driver

		for {
			select {
			case <-p.sigTerm:
				return
			case msg := <-p.out:
				message, driverPtr := p.decodeMessage(msg)
				if driverPtr == nil {
					driverPtr = first
				}

			DriverLoop:
				for _, d := range p.drivers {

					if reflect.DeepEqual(driverPtr, d.driver) {
						d.out <- message
						break DriverLoop
					}
				}
			}
		}

	}()

	return nil
}

// CloseStream closes each drivers streams by stopping the relaying
// go routines
func (p *syntheticDriver) CloseStream() error {

	for _, d := range p.drivers {

		p.sigTerm <- 1

		if d.executionFlag == ConcurrentExecution {
			p.sigTerm <- 1
		}

		defer d.driver.CloseStream()
	}

	// signal for send relay operation
	p.sigTerm <- 1

	return nil
}

// GetMessageWriterChannel returns the channel to write message to be
// published to
func (p *syntheticDriver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return p.out, nil
}

// GetMessageReaderChannel returns the channel to receive received messages
// from
func (p *syntheticDriver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return p.in, nil
}
