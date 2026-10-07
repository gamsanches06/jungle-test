package failpoint

import (
	"errors"
	"testing"
)

func TestFailpoints(t *testing.T) {
	defer Reset()
	if err := Inject("x"); err != nil {
		t.Fatal(err)
	}
	if err := Parse("a=error;b=once-error"); err != nil {
		t.Fatal(err)
	}
	if err := Inject("a"); !errors.Is(err, ErrInjected) {
		t.Fatal(err)
	}
	if err := Inject("b"); !errors.Is(err, ErrInjected) {
		t.Fatal(err)
	}
	if err := Inject("b"); err != nil {
		t.Fatal("once-error fired twice")
	}
	var code int
	exitFn = func(c int) { code = c }
	_ = Enable("c", "exit")
	_ = Inject("c")
	if code != 137 || Hits("c") != 1 {
		t.Fatalf("exit code %d hits %d", code, Hits("c"))
	}
	if Parse("bad") == nil || Enable("d", "explode") == nil {
		t.Fatal("invalid specs accepted")
	}
}
