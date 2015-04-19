# CasLock [![GoDoc](https://godoc.org/github.com/jroimartin/caslock?status.svg)](https://godoc.org/github.com/jroimartin/caslock)

## Description

Package caslock implements a row lock mechanism for cassandra, based on
lightweight transactions. It allows to lock an arbitrary number of rows,
protecting them from concurrent access.

## Usage

```go
l, err := caslock.Acquire(session, "keyspace", "table",
	30 * time.Second, "rowKey1", "rowKey2")
if err != nil {
	return err
}
defer l.Release()
```

## Installation

`go get github.com/jroimartin/caslock`

## More information

`godoc github.com/jroimartin/caslock`
