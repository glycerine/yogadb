package yogadb

import (
	//"fmt"
	"strconv"
)

// formatWithUnderscores formats an int64 with underscores every 3 digits.
func formatWithUnderscores(n int64) string {
	str := strconv.FormatInt(n, 10)

	startOffset := 0
	if n < 0 {
		startOffset = 1 // Skip the minus sign for counting
	}

	digitCount := len(str) - startOffset
	if digitCount <= 3 {
		return str
	}

	// Calculate how many underscores we need
	numOfUnderscores := (digitCount - 1) / 3
	result := make([]byte, len(str)+numOfUnderscores)

	if startOffset == 1 {
		result[0] = '-'
	}

	// Iterate backwards through the string, inserting underscores
	writeIdx := len(result) - 1
	digitsProcessed := 0

	for i := len(str) - 1; i >= startOffset; i-- {
		result[writeIdx] = str[i]
		writeIdx--
		digitsProcessed++

		// Insert an underscore every 3 digits, unless we are at the front
		if digitsProcessed == 3 && i > startOffset {
			result[writeIdx] = '_'
			writeIdx--
			digitsProcessed = 0
		}
	}

	return string(result)
}
