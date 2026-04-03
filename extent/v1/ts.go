package v1

import "github.com/zaibyte/zaipkg/xtime/hlc"

// getTimestamp gets a uint64 logic timestamp which never go backwards.
func getTimestamp() uint64 {
	return hlc.Next()
}
