package brokerutil

import (
	"errors"
	"fmt"
	"log"
	"reflect"
)

type pubSubDriverMergerDriverMeta struct {
	executionFlag Flag
	driver        PubSubDriverScaffold
}

// PubSubDriverMerger is a ConcurrentPubSubDriverScaffold compliant
// pub sub driver which merges other pub sub drivers of the same type
// into one
type PubSubDriverMerger struct {
	executionFlag Flag
	drivers       []pubSubDriverMergerDriverMeta
	transmitChan  chan interface{}
	receiveChan   chan interface{}
	signalChan    chan int
}

// NewPubSubDriverMerger creates a new driver merger which merges multiple drivers
// into one to be used as PubSub driver.
//
// The first driver is used to publish messages
func NewPubSubDriverMerger(drivers ...PubSubDriverScaffold) (*PubSubDriverMerger, error) {

	if len(drivers) == 0 {
		return nil, errors.New("cannot create driver merger with no drivers")
	}

	// set executionFlag, check all drivers on compliance
	var executionFlag Flag = -1
	var metaDriverSlice = make([]pubSubDriverMergerDriverMeta, 0)
	for _, d := range drivers {

		if containsFlag(d.GetDriverFlags(), RequiresBlockingExecution) {
			metaDriverSlice = append(metaDriverSlice, pubSubDriverMergerDriverMeta{
				executionFlag: RequiresBlockingExecution,
				driver:        d,
			})

		} else if containsFlag(d.GetDriverFlags(), RequiresConcurrentExecution) {
			metaDriverSlice = append(metaDriverSlice, pubSubDriverMergerDriverMeta{
				executionFlag: RequiresConcurrentExecution,
				driver:        d,
			})
		} else {
			return nil, fmt.Errorf("driver %v does not return execution flag when calling GetDriverFlags()", d)
		}
	}

	return &PubSubDriverMerger{
		drivers:       metaDriverSlice,
		executionFlag: executionFlag,
		transmitChan:  make(chan interface{}, 1),
		receiveChan:   make(chan interface{}, 1),
		signalChan:    make(chan int),
	}, nil

}

// GetDriverFlags returns the drivers flags to signal the type of execution
func (p PubSubDriverMerger) GetDriverFlags() []Flag {
	return []Flag{RequiresConcurrentExecution}
}

// OpenStream opens each drivers streams and the relays those streams
// onto the mergers own streams
func (p PubSubDriverMerger) OpenStream() error {

	pubDriverPtr := &p.drivers[0].driver

	for _, d := range p.drivers {

		if err := d.driver.OpenStream(); err != nil {
			return err
		}

		if d.executionFlag == RequiresBlockingExecution {

			driver, ok := d.driver.(BlockingPubSubDriverScaffold)
			if !ok {
				return fmt.Errorf("cannot parse driver %v to BlockingPubSubDriverScaffold", d)
			}

			go func() {

				defer driver.CloseStream()

				// check if it is the first driver which will publish
				if reflect.DeepEqual(pubDriverPtr, &d.driver) {

					for {
						select {

						case <-p.signalChan:
							return

						case msg := <-p.transmitChan:
							err := driver.PublishMessage(msg)
							if err != nil {
								log.Printf("%v", err)
							}

						default:
							msg, err := driver.ReceiveMessage()
							if err != nil {
								log.Printf("%v", err)
							}

							p.receiveChan <- msg
						}
					}

				} else {

					for {
						select {
						case <-p.signalChan:
							return
						default:
							msg, err := driver.ReceiveMessage()
							if err != nil {
								log.Printf("%v", err)
							}

							p.receiveChan <- msg
						}
					}
				}

			}()

		} else if p.executionFlag == RequiresConcurrentExecution {

			driver, ok := d.driver.(ConcurrentPubSubDriverScaffold)
			if !ok {
				return fmt.Errorf("cannot parse driver %v to ConcurrentPubSubDriverScaffold", d)
			}

			if reflect.DeepEqual(pubDriverPtr, &d.driver) {

				go func() {

					// dont defer, CloseStream() will be called in the
					// rx routine

					driverTransmitChan, _ := driver.GetMessageWriterChannel()

					for {
						select {
						case <-p.signalChan:
							return
						case msg := <-p.transmitChan:
							driverTransmitChan <- msg
						}
					}
				}()
			}

			go func() {

				defer driver.CloseStream()

				driverReceiveChan, _ := driver.GetMessageReaderChannel()

				for {
					select {
					case <-p.signalChan:
						return
					case msg := <-driverReceiveChan:
						p.receiveChan <- msg
					}
				}
			}()
		}
	}

	return nil
}

// CloseStream closes each drivers streams by stopping the relaying
// go routines
func (p PubSubDriverMerger) CloseStream() error {

	var pubDriverPtr = &p.drivers[0].driver
	for _, d := range p.drivers {

		p.signalChan <- 1

		if d.executionFlag == RequiresConcurrentExecution && reflect.DeepEqual(pubDriverPtr, &d.driver) {
			p.signalChan <- 1
		}
	}

	return nil
}

// GetMessageWriterChannel returns the channel to write message to be
// published to
func (p PubSubDriverMerger) GetMessageWriterChannel() (chan<- interface{}, error) {
	return p.transmitChan, nil
}

// GetMessageReaderChannel returns the channel to receive received messages
// from
func (p PubSubDriverMerger) GetMessageReaderChannel() (<-chan interface{}, error) {
	return p.receiveChan, nil
}
