package brokerutil

func contains(slice []interface{}, key interface{}, compareFn func(interface{}, interface{}) bool) bool {

	for _, s := range slice {

		if compareFn(key, s) {
			return true
		}
	}

	return false
}
