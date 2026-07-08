package core

import (
	"errors"
	"testing"
)

func TestValidateControlRequestRejectsNilRequest(t *testing.T) {
	t.Parallel()

	err := ValidateControlRequest(nil)
	if !errors.Is(err, ErrInvalidControlOperation) {
		t.Fatalf("error = %v, want ErrInvalidControlOperation", err)
	}
}
