# brokerutil
brokerutil provides a common interface to message-brokers for pub-sub applications.

[![GoDoc](https://godoc.org/github.com/jakoblorz/brokerutil?status.svg)](https://godoc.org/github.com/jakoblorz/brokerutil)
[![Build Status](https://travis-ci.com/jakoblorz/brokerutil.svg?branch=master)](https://travis-ci.com/jakoblorz/brokerutil)
[![codecov](https://codecov.io/gh/jakoblorz/brokerutil/branch/master/graph/badge.svg)](https://codecov.io/gh/jakoblorz/brokerutil)

Use brokerutil to be able to build pub-sub applications which are not
highly dependent on the message-brokers drivers implementation.
brokerutil provides a common interface which enables developers to switch
the message broker without having to rewrite major parts of the applications
pub-sub logic.

## Installation

`go get github.com/jakoblorz/brokerutil`

Use the native drivers in the driver package or implement your own. Currently provided drivers:
- [redis](https://redis.io/) `go get github.com/jakoblorz/brokerutil/driver/redis`
- loopback (for testing) `go get github.com/jakoblorz/brokerutil/driver/loopback`

## Example
This example will use redis as message-broker. It uses the brokerutil native redis driver which
relies on the [github.com/go-redis/redis](http://github.com/go-redis/redis) driver.

```go
package main

import (
    "github.com/jakoblorz/brokerutil"
    "github.com/jakoblorz/brokerutil/redis"
    r "github.com/go-redis/redis"
)

func main() {

    // create redis driver to support pub-sub
    driver, err := redis.NewDriver(&redis.DriverOptions{
        channel: "brokerutil-chan",
        driver:  &r.Options{
            Addr:     "localhost:6379",
            Password: "",
            DB:       0
        },
    })

    if err != nil {
        log.Fatalf("could not create brokerutil redis driver: %v", err)
        return
    }

    // create new pub sub using the initialized redis driver
    ps, err := brokerutil.NewPubSubFromDriver(driver)
    if err != nil {
        log.Fatalf("could not create brokerutil pub sub: %v", err)
        return
    }

    // run blocking subscribe as go routine
    go ps.SubscribeSync(func(msg interface{}) error {
        fmt.Printf("%v", msg)

        return nil
    })

    // start controller routine which blocks execution
    if err := ps.ListenSync(); err != nil {
        log.Fatalf("could not run controller routine: %v", err)
        return
    }
}
```
