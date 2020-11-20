# `s3c4`

[![GoDoc](https://godoc.org/github.com/Avalanche-io/s3c4?status.svg)](https://godoc.org/github.com/avalanche-io/s3c4)

A Go package implementing the c4 Store interface for AWS s3.

### Example Usage

```go
package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/s3c4"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {

	bucket := "your_bucket"
	s3svc := s3.New(session.New())
	store, err := s3c4.New(s3svc, bucket, "optional_prefix")
	DontPanic(err)

	data := []byte("foobar")
	id := c4.Identify(bytes.NewReader(data))

	// Open S3 Object for writing, error if object already exists.
	w, err := store.Create(id)
	DontPanic(err)

	// Write data to object
	_, err = io.Copy(w, bytes.NewReader(data))
	DontPanic(err)

	// Closing to flush write, so we can read it back immedeatly.
	// Check for error on close when using any C4 Store.
	err = w.Close()
	DontPanic(err)

	buffer := make([]byte, 512)

	// Open S3 Object for reading.
	r, err := store.Open(id)
	DontPanic(err)

	n, err := r.Read(buffer)
	DontPanic(err)

	err = r.Close()
	DontPanic(err)

	fmt.Printf("%s\n", string(buffer[:n]))
	// output: "foobar"

	// Delete S3 Object
	err = store.Remove(id)
	DontPanic(err)

	err = store.Close()
	DontPanic(err)
}

func DontPanic(err error) {
  if err == nil {
    return
  }
	panic(err)
}

```

