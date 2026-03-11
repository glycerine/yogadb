package yogadb

import (
	//"fmt"
	"testing"
)

func TestFormatWithUnderscores(t *testing.T) {
	// Test cases
	assert("123" == formatInt64Under(123))                    // 123
	assert("1_000" == formatInt64Under(1000))                 // 1_000
	assert("1_000_000" == formatInt64Under(1000000))          // 1_000_000
	assert("-9_876_543_210" == formatInt64Under(-9876543210)) // -9_876_543_210

	assert(formatF64Under(1234567.89) == "1_234_567.89")
	assert(formatF64Under(1000.5) == "1_000.5")
	assert(formatF64Under(-9876543.2105) == "-9_876_543.2105")
	assert(formatF64Under(0.12345) == "0.12345")
	assert(formatF64Under(1000000) == "1_000_000")

}
