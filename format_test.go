package yogadb

import (
	//"fmt"
	"testing"
)

func TestFormatWithUnderscores(t *testing.T) {
	// Test cases
	assert("123" == formatWithUnderscores(123))                    // 123
	assert("1_000" == formatWithUnderscores(1000))                 // 1_000
	assert("1_000_000" == formatWithUnderscores(1000000))          // 1_000_000
	assert("-9_876_543_210" == formatWithUnderscores(-9876543210)) // -9_876_543_210
}
