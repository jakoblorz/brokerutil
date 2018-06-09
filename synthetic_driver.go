package brokerutil

import (
	"fmt"
	"log"
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
	driver        PubSubDriverScaffold
	out           chan interface{}
}

type syntheticDriverOptions struct {
	UseSyntheticMessageWithSource bool
	UseSyntheticMessageWithTarget bool
}

type syntheticMessageWithSource struct {
	source  PubSubDriverScaffold
	message interface{}
}

type syntheticMessageWithTarget struct {
	target  PubSubDriverScaffold
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
func newSyntheticDriver(options *syntheticDriverOptions, drivers ...PubSubDriverScaffold) (*syntheticDriver, error) {

	// set executionFlag, check all drivers on compliance
	var metaDriverSlice = make([]metaDriverWrapper, 0)
	for _, d := range drivers {

		if containsFlag(d.GetDriverFlags(), RequiresBlockingExecution) {
			metaDriverSlice = append(metaDriverSlice, metaDriverWrapper{
				executionFlag: RequiresBlockingExecution,
				driver:        d,
				out:           make(chan interface{}, 1),
			})

		} else if containsFlag(d.GetDriverFlags(), RequiresConcurrentExecution) {
			metaDriverSlice = append(metaDriverSlice, metaDriverWrapper{
				executionFlag: RequiresConcurrentExecution,
				driver:        d,
				out:           make(chan interface{}, 1),
			})
		} else {
			return nil, fmt.Errorf("driver %v does not return execution flag when calling GetDriverFlags()", d)
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

func (p *syntheticDriver) encodeMessage(msg interface{}, d PubSubDriverScaffold) interface{} {

	if p.getOptions().UseSyntheticMessageWithSource {
		return syntheticMessageWithSource{
			message: msg,
			source:  d,
		}
	}

	return msg
}

func (p *syntheticDriver) decodeMessage(msg interface{}) (interface{}, PubSubDriverScaffold) {

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
	return []Flag{RequiresConcurrentExecution}
}

// OpenStream opens each drivers streams and the relays those streams
// onto the mergers own streams
func (p *syntheticDriver) OpenStream() error {

	for _, d := range p.drivers {

		if err := d.driver.OpenStream(); err != nil {
			return err
		}

		var s = &sync.WaitGroup{}

		if d.executionFlag == RequiresBlockingExecution {

			driver, ok := d.driver.(BlockingPubSubDriverScaffold)
			if !ok {
				return fmt.Errorf("cannot parse driver %v to BlockingPubSubDriverScaffold", d)
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
						err := driver.PublishMessage(msg)
						if err != nil {
							log.Printf("%v", err)
						}

					default:
						msg, err := driver.ReceiveMessage()
						if err != nil {
							log.Printf("%v", err)
						}

						if msg != nil {
							p.in <- p.encodeMessage(msg, d.driver)
						}
					}
				}

			}()

		} else if d.executionFlag == RequiresConcurrentExecution {

			driver, ok := d.driver.(ConcurrentPubSubDriverScaffold)
			if !ok {
				return fmt.Errorf("cannot parse driver %v to ConcurrentPubSubDriverScaffold", d)
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

		if d.executionFlag == RequiresConcurrentExecution {
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
