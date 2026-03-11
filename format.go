package yogadb

import (
	//"fmt"
	"strconv"
	"strings"
)

// formatInt64Under formats an int64 with underscores every 3 digits.
func formatInt64Under(n int64) string {
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

// formatF64Under formats a float64 with underscores in the integer part.
func formatF64Under(f float64) string {
	// Format float to string without scientific notation.
	// -1 precision means it uses the minimum number of digits necessary.
	str := strconv.FormatFloat(f, 'f', -1, 64)

	// Split the string into integer and fractional parts
	parts := strings.Split(str, ".")
	intPart := parts[0]

	// Handle the negative sign for the integer part
	startOffset := 0
	if len(intPart) > 0 && intPart[0] == '-' {
		startOffset = 1
	}

	digitCount := len(intPart) - startOffset
	if digitCount <= 3 {
		return str // No underscores needed, return the original formatted float
	}

	// Calculate how many underscores we need for the integer part
	numOfUnderscores := (digitCount - 1) / 3
	intResult := make([]byte, len(intPart)+numOfUnderscores)

	if startOffset == 1 {
		intResult[0] = '-'
	}

	// Iterate backwards through the integer part, inserting underscores
	writeIdx := len(intResult) - 1
	digitsProcessed := 0

	for i := len(intPart) - 1; i >= startOffset; i-- {
		intResult[writeIdx] = intPart[i]
		writeIdx--
		digitsProcessed++

		if digitsProcessed == 3 && i > startOffset {
			intResult[writeIdx] = '_'
			writeIdx--
			digitsProcessed = 0
		}
	}

	// Reconstruct the final string with the fractional part, if it exists
	if len(parts) > 1 {
		return string(intResult) + "." + parts[1]
	}

	return string(intResult)
}

func main() {
	// Test cases
	assert(formatF64Under(1234567.89) == "1_234_567.89")
	assert(formatF64Under(1000.5) == "1_000.5")
	assert(formatF64Under(-9876543.2105) == "-9_876_543.2105")
	assert(formatF64Under(0.12345) == "0.12345")
	assert(formatF64Under(1000000) == "1_000_000")
}
