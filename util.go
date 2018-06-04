package brokerutil

import "github.com/jakoblorz/brokerutil/driver"

func containsFlag(slice []driver.Flag, flag driver.Flag) bool {

	for _, s := range slice {

		if s == flag {
			return true
		}
	}

	return false
}
