# singapoor
singapoor provides a common interface to message-brokers for pub-sub applications.

Use singapoor to be able to build pub-sub applications which are not
highly dependent on the message-brokers driver implementation quirks.
singapoor provides a common interface which enables developers to switch
the message broker without having to rewrite major parts of the applications
pub-sub logic.

## Example using redis pub-sub as message broker
```go
package main

import (
  "github.com/jakoblorz/singapoor"
  "github.com/jakoblorz/singapoor/driver/redis"
  goredis "github.com/go-redis/redis"
)

func main() {

  // create redis driver to support pub-sub
  driver, err := redis.NewDriver(&redis.DriverOptions{
    channel: "singapoor-chan",
    driver:  &goredis.Options{
      Addr:     "localhost:6379",
      Password: "",
      DB:       0
    },
  })

  if err != nil {
    log.Fatalf("could not create singapoor redis driver: %v", err)
    return
  }

  // create new pub sub using the initialized redis driver
  ps, err := singapoor.NewPubSubFromDriver(driver)
  if err != nil {
    log.Fatalf("could not create singapoor pub sub: %v", err)
    return
  }

  // run blocking subscribe as go routine
  go ps.SubscribeSync(func(msg interface{}) error {
    fmt.Printf("%s", msg)

    return nil
  })

  // start controller routine which blocks execution
  if err := controller.Listen(); err != nil {
    log.Fatalf("could not run controller routine: %v", err)
    return
  }

}
```
