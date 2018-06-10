# [brokerutil](https://github.com/jakoblorz/brokerutil)
brokerutil provides a common interface to message-brokers for pub-sub applications.

[![GoDoc](https://godoc.org/github.com/jakoblorz/brokerutil?status.svg)](https://godoc.org/github.com/jakoblorz/brokerutil)
[![Build Status](https://travis-ci.com/jakoblorz/brokerutil.svg?branch=master)](https://travis-ci.com/jakoblorz/brokerutil)
[![codecov](https://codecov.io/gh/jakoblorz/brokerutil/branch/master/graph/badge.svg)](https://codecov.io/gh/jakoblorz/brokerutil)

Use brokerutil to be able to build pub-sub applications which feature interopability with message-brokers drivers.
brokerutil provides a abstract and extendible interface which enables developers to switch or mix
message brokers without having to rewrite major parts of the applications
pub-sub logic.

## Features
- sync / async subscription
- multi-driver support - subscribe to messages from multiple brokers
- simple yet extendible pub sub interface
- simple driver interface to implement own drivers

### Preinstalled Drivers
- loopback
- [redis](https://redis.io/) based on [github.com/go-redis/redis](http://github.com/go-redis/redis)
- [kafka](https://kafka.apache.org/) (soon [#1](https://github.com/jakoblorz/brokerutil/pull/1)) based on [github.com/Shopify/sarama](https://github.com/Shopify/sarama)

## Installation

`go get github.com/jakoblorz/brokerutil`

Use the package drivers or implement your own.

## Example
This example will use redis as message-broker. It uses the package redis driver which
extends on the [github.com/go-redis/redis](http://github.com/go-redis/redis) driver.

```go
package main

import (
    "flag"

    "github.com/jakoblorz/brokerutil"
    "github.com/jakoblorz/brokerutil/redis"
    r "github.com/go-redis/redis"
)

var (
    raddr   = flag.String("raddr", ":6379", "redis address to connect to")
    channel = flag.String("channel", "brokerutil", "redis message channel")
)

func main() {
    flag.Parse()

    // create redis driver to support pub-sub
    driver, err := redis.NewRedisPubSubDriver([]string{*channel}, &r.Options{
        Addr: *raddr,
    })

    if err != nil {
        log.Fatalf("could not create redis driver: %v", err)
    }

    // create new pub sub using the initialized redis driver
    ps, err := brokerutil.NewPubSubFromDriver(driver)
    if err != nil {
        log.Fatalf("could not create pub sub: %v", err)
    }

    // run blocking subscribe as go routine
    go ps.SubscribeSync(func(msg interface{}) error {
        fmt.Printf("%v", msg)

        return nil
    })

    // start controller routine which blocks execution
    if err := ps.ListenSync(); err != nil {
        log.Fatalf("could not listen: %v", err)
    }
}
```
