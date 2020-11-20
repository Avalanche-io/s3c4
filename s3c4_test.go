package s3c4

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

func TestS3c4(t *testing.T) {
	godotenv.Load()

	access := os.Getenv("TEST_AWS_ACCESS_KEY")
	secret := os.Getenv("TEST_AWS_SECRET_KEY")
	region := os.Getenv("TEST_AWS_REGION")
	bucket := os.Getenv("TEST_AWS_BUCKET")
	if len(access) == 0 || len(secret) == 0 || len(region) == 0 || len(bucket) == 0 {
		t.Fatal("Missing env vars, see .env.example")
	}

	ses, err := session.NewSession(&aws.Config{
		Region:      &region,
		Credentials: credentials.NewStaticCredentials(access, secret, ""),
	})
	if err != nil {
		t.Fatal(err)
	}

	store, err := New(s3.New(ses), bucket, "store_test")
	if err != nil {
		t.Fatal(err)
	}

	testdata := make(map[string]c4.ID)
	for i := 0; i < 5; i++ {

		// Create arbitrary test data
		data := []byte(fmt.Sprintf("%06d", i))

		// Create c4 id of the test data
		id := c4.Identify(bytes.NewReader(data))
		testdata[string(data)] = id

		// Test store `Create` method
		w, err := store.Create(id)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			t.Fatal(err)
		}

		// Write data to store
		_, err = w.Write([]byte(data))
		if err != nil {
			t.Fatal(err)
		}

		// Close the store
		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}

	}

	// Test store `Open` method
	for k, v := range testdata {

		f, err := store.Open(v)
		if err != nil {
			t.Fatal(err)
			continue
		}

		data := make([]byte, 512)
		n, err := f.Read(data)
		if err != nil {
			t.Fatal(err)
		}

		data = data[:n]
		if string(data) != k {
			t.Fatalf("wrong data read from file, expected %q, got %q", k, string(data))
		}

		f.Close()
	}

	// Test store `Remove`
	for _, v := range testdata {
		err := store.Remove(v)
		if err != nil {
			t.Error(err)
		}
	}

	// Closing insures all writers are closed.
	store.Close()
}
