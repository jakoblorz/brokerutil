# singapoor
create a mesh network of stream processing nodes allowing for simple pub/sub

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
  go ps.BlockingSubscribe(func(msg interface{}) error {
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
