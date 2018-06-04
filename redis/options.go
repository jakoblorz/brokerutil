package redis

import (
	"github.com/go-redis/redis"
)

type DriverOptions struct {
	driver  *redis.Options
	channel string
}
