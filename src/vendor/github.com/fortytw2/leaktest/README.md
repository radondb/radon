Leaktest [![Build Status](https://travis-ci.org/fortytw2/leaktest.svg?branch=master)](https://travis-ci.org/fortytw2/leaktest)
------

Refactored, tested variant of the goroutine leak detector found in both `net/http` tests and the `cockroachdb`
source tree.

Takes a snapshot of running goroutines at the start of a test, and at the end -
compares the two and *voila*. Ignores runtime/sys goroutines. Doesn't play nice
with `t.Parallel()` right now, but there are plans to do so.

### Installation

```
go get -u github.com/fortytw2/leaktest
```

### Example

This test fails, because it leaks a goroutine :o

```go
func TestPool(t *testing.T) {
	defer leaktest.Check(t)()

    go func() {
        for {
            time.Sleep(time.Second)
        }
    }()
}
```


LICENSE
------
Same BSD-style as Go, see LICENSE
