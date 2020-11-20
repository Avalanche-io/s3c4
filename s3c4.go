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

// S3 is a Amazon Simple Storage Solution object storage implementation of
// c4 Store.
type Store struct {
	s3 s3iface.S3API

	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader

	// s3 bucket
	bucket string

	// key prefix
	prefix string

	wg sync.WaitGroup
}

//  New - creates a c4 Store backed by any AWS S3 API compatible object storage
// backend. Objects will be stored in the `bucket` specified, and `keyprefix`
// is prepended to the c4 id separated by a slash "/". If no `keyprefix` is
// provided then key will be the c4 id.
func New(s3api s3iface.S3API, bucket string, keyprefix string) (*Store, error) {

	// Create an uploader with S3 client and default options
	s := new(Store)
	s.s3 = s3api
	s.uploader = s3manager.NewUploaderWithClient(s3api)
	s.downloader = s3manager.NewDownloaderWithClient(s3api)
	s.bucket = bucket
	s.prefix = keyprefix

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
			s.wg.Done()
			w.Close()
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

func (w *s3WriteCloser) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	err := w.w.Close()
	if err != nil {
		return err
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
			case <-time.After(time.Millisecond * 500):
			}
		}
	}()

	select {
	case <-time.After(time.Second * 5):
	case <-headok:
	}
	return nil
}

func (s *Store) Create(id c4.ID) (io.WriteCloser, error) {
	key := id.String()
	if len(s.prefix) > 0 {
		key = filepath.Join(s.prefix, key)
	}

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	resp, err := s.s3.HeadObject(headInput)
	if err == nil {
		return nil, &os.PathError{Op: "create", Path: key, Err: os.ErrExist}
	}
	_ = resp

	s.wg.Add(1)
	r, w := io.Pipe()

	out := &s3WriteCloser{s.s3, w, false, headInput}

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
