package brokerutil

func containsFlag(slice []Flag, flag Flag) bool {

	for _, s := range slice {

		if s == flag {
			return true
		}
	}

	return false
}
