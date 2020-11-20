package s3c4

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	filepath "path"
	"sync"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// s3c4 is an AWS S3 object storage implementation of the c4 Store interface.
type Store struct {
	// ConfirmCreate indecates if the io.WriteCloser returned by Create should
	// block on Close until a HeadObject request succedes.
	// The default is `true`, set to `false` to return immedeatly without checks.
	ConfirmCreate bool

	// ConfirmationTimeout is the maximum amount of time to block waiting for
	// object creation confirmation. If the timeout is exceded then Close returns
	// an error.
	// The default is 5 seconds.
	ConfirmationTimeout time.Duration

	// ConfirmationRequestRate is the amount of time between HeadObject requests.
	// The default is 500 milliseconds
	ConfirmationRequestRate time.Duration

	s3         s3iface.S3API
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
	prefix     string
	wg         sync.WaitGroup
}

// New - creates a c4 Store backed by any AWS S3 API compatible object storage
// backend. Objects will be stored in the `bucket` specified, and `keyprefix`
// is prepended to the c4 id separated by a slash "/". If no `keyprefix` is
// provided then key will only be the c4 id.
func New(s3api s3iface.S3API, bucket string, keyprefix string) (*Store, error) {

	// Create an uploader with S3 client and default options
	s := &Store{
		ConfirmCreate:           true,
		ConfirmationTimeout:     time.Second * 5,
		ConfirmationRequestRate: time.Millisecond * 500,

		s3:         s3api,
		uploader:   s3manager.NewUploaderWithClient(s3api),
		downloader: s3manager.NewDownloaderWithClient(s3api),
		bucket:     bucket,
		prefix:     keyprefix,
	}

	// By convention for c4 Store `New` returns an error.
	return s, nil
}

func (s *Store) Open(id c4.ID) (io.ReadCloser, error) {
	key := id.String()
	if len(s.prefix) > 0 {
		key = filepath.Join(s.prefix, key)
	}

	// Perform an upload.
	buff := &aws.WriteAtBuffer{}
	n, err := s.downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("%s %s (n == %d)", key, err, n)
	}

	r, w := io.Pipe()

	s.wg.Add(1)
	go func() {
		defer func() {
			err := w.Close()
			if err != nil {
				r.CloseWithError(err)
			}
			s.wg.Done()
		}()

		_, err = io.Copy(w, bytes.NewReader(buff.Bytes()))
		if err != nil {
			r.CloseWithError(err)
			return
		}
	}()

	return ioutil.NopCloser(r), nil
}

type s3WriteCloser struct {
	confirmCreate           bool
	confirmationTimeout     time.Duration
	confirmationRequestRate time.Duration

	id c4.ID

	s3 s3iface.S3API

	// write pipe
	w io.WriteCloser

	closed bool

	// prepared HeadObjectInput for HEAD requests
	headInput *s3.HeadObjectInput
}

func (w *s3WriteCloser) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

// ErrConfirmationTimeout is the error type returned on Close if object creation
// could not be confirmed before the `ConfirmationTimeout` elapsed.
type ErrConfirmationTimeout struct {
	Id                  c4.ID
	ConfirmationTimeout time.Duration
}

func (e ErrConfirmationTimeout) Error() string {
	return fmt.Sprintf("confirmation timed out after %s writting %s",
		e.ConfirmationTimeout, e.Id)
}

func (w *s3WriteCloser) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	err := w.w.Close()
	if err != nil {
		return err
	}

	if !w.confirmCreate {
		return nil
	}

	headok := make(chan struct{})
	done := make(chan struct{})
	defer close(done)

	go func() {
		defer close(headok)
		for {
			_, err := w.s3.HeadObject(w.headInput)
			if err == nil {
				return
			}
			select {
			case <-done:
				return
			case <-time.After(w.confirmationRequestRate):
			}
		}
	}()

	select {
	case <-time.After(w.confirmationTimeout):
		return ErrConfirmationTimeout{w.id, w.confirmationTimeout}
	case <-headok:
	}
	return nil
}

// Create returns a io.WriteCloser that blocks when Closed until the object
// header can be read.
func (s *Store) Create(id c4.ID) (io.WriteCloser, error) {
	key := id.String()
	if len(s.prefix) > 0 {
		key = filepath.Join(s.prefix, key)
	}

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	// Check if object exists
	resp, err := s.s3.HeadObject(headInput)
	if err == nil {
		return nil, &os.PathError{Op: "create", Path: key, Err: os.ErrExist}
	}
	_ = resp

	r, w := io.Pipe()
	out := &s3WriteCloser{
		s.ConfirmCreate, s.ConfirmationTimeout, s.ConfirmationRequestRate,
		id, s.s3, w, false, headInput,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		_, err := s.uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   r,
		})
		if err != nil {
			r.CloseWithError(err)
		}
	}()

	return out, nil
}

func (s *Store) Close() error {
	s.wg.Wait()
	return nil
}

func (s *Store) Remove(id c4.ID) error {
	path := filepath.Join(s.prefix, id.String())
	_, err := s.s3.DeleteObject(&s3.DeleteObjectInput{Bucket: &s.bucket, Key: &path})
	return err
}
