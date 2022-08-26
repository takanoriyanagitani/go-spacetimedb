package stdb

import (
	"testing"
	"time"
)

func TestYmdConverter(t *testing.T) {
	t.Parallel()
	t.Run("epoch", func(t *testing.T) {
		t.Parallel()
		tm := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
		var converted string = YmdConverter(tm)
		expected := "19700101"
		if expected != converted {
			t.Errorf("Unexpected value got.\n")
			t.Errorf("Expected: %s\n", expected)
			t.Errorf("Got: %s\n", converted)
		}
	})
}
