package blobfs

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
	gcaws "gocloud.dev/aws"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"
)

const (
	awsRoleArn              = "AWS_ROLE_ARN"
	awsWebIdentityTokenFile = "AWS_WEB_IDENTITY_TOKEN_FILE"
)

type (
	// BlobFS provides a filesystem abstraction over various blob storage backends
	BlobFS struct {
		storageURL  string
		storageType string
		prefix      string
		hook        TelemetryHook
		CACert      []byte
	}

	// TelemetryHook is invoked after each filesystem operation
	// It is intended for metrics, tracing, and observability
	TelemetryHook func(
		ctx context.Context,
		op Operation,
		dirName string,
		fileName string,
		storageType string,
		err error,
	)
)

// Operation represents a filesystem operation
type Operation int

const (
	Write Operation = iota
	Read
	Delete
	Exists
	SignedURL
	OpenBucket
)

func (op Operation) String() string {
	switch op {
	case Write:
		return "write_file"
	case Read:
		return "read_file"
	case Delete:
		return "delete_file"
	case Exists:
		return "exists"
	case SignedURL:
		return "signed_url"
	case OpenBucket:
		return "open_bucket"
	default:
		return fmt.Sprintf("unknown_operation(%d)", int(op))
	}
}

func New(storageURL string, prefix ...string) *BlobFS {
	var bucketPrefix string
	if len(prefix) > 0 {
		bucketPrefix = prefix[0]
	}

	return &BlobFS{
		storageURL:  storageURL,
		storageType: extractStorageType(storageURL),
		hook:        nil,
		prefix:      bucketPrefix,
	}
}

// NOTE: Any telemetry hook must be registered right after calling New()
// NOT safe for concurrent use
func (b *BlobFS) WithMetricsHook(metricsHook TelemetryHook) *BlobFS {
	b.hook = metricsHook
	return b
}

var _ Interface = (*BlobFS)(nil)

func NewInMemoryFS() Interface {
	return New("mem://")
}

func NewOsFs() Interface {
	return New("file:///")
}

func (fs *BlobFS) WriteFile(ctx context.Context, filepath string, data []byte) (err error) {
	dirName, fileName := path.Split(filepath)
	defer func() {
		if fs.hook != nil {
			fs.hook(ctx, Write, dirName, fileName, fs.storageType, err)
		}
	}()
	return fs.writeFile(ctx, dirName, fileName, data)
}

func (fs *BlobFS) writeFile(ctx context.Context, dirName, fileName string, data []byte) error {
	bucket, err := fs.openBucket(ctx, dirName)
	if err != nil {
		return err
	}
	defer bucket.Close()

	w, err := bucket.NewWriter(ctx, fileName, &blob.WriterOptions{
		DisableContentTypeDetection: true,
	})
	if err != nil {
		return err
	}
	_, writeErr := w.Write(data)
	// Always check the return value of Close when writing.
	closeErr := w.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

func (fs *BlobFS) ReadFile(ctx context.Context, filepath string) (bytes []byte, err error) {
	dirName, fileName := path.Split(filepath)
	defer func() {
		if fs.hook != nil {
			fs.hook(ctx, Read, dirName, fileName, fs.storageType, err)
		}
	}()
	return fs.readFile(ctx, dirName, fileName)
}

func (fs *BlobFS) readFile(ctx context.Context, dirName, fileName string) ([]byte, error) {
	bucket, err := fs.openBucket(ctx, dirName)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()
	// Open the key "foo.txt" for reading with the default options.
	r, err := bucket.NewReader(ctx, fileName, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (fs *BlobFS) DeleteFile(ctx context.Context, filepath string) (err error) {
	dirName, fileName := path.Split(filepath)
	defer func() {
		if fs.hook != nil {
			fs.hook(ctx, Delete, dirName, fileName, fs.storageType, err)
		}
	}()
	return fs.deleteFile(ctx, dirName, fileName)
}

func (fs *BlobFS) deleteFile(ctx context.Context, dirName, fileName string) error {
	bucket, err := fs.openBucket(ctx, dirName)
	if err != nil {
		return err
	}
	defer bucket.Close()

	err = bucket.Delete(context.TODO(), fileName)
	return err
}

func (fs *BlobFS) Exists(ctx context.Context, filepath string) (exists bool, err error) {
	dirName, filename := path.Split(filepath)
	defer func() {
		if fs.hook != nil {
			fs.hook(ctx, Exists, dirName, filename, fs.storageType, err)
		}
	}()
	return fs.exists(ctx, dirName, filename)
}

func (fs *BlobFS) exists(ctx context.Context, dirName, fileName string) (bool, error) {
	bucket, err := fs.openBucket(ctx, dirName)
	if err != nil {
		return false, err
	}
	defer bucket.Close()

	exists, err := bucket.Exists(context.TODO(), fileName)
	return exists, err
}

func (fs *BlobFS) SignedURL(ctx context.Context, filepath string, opts *blob.SignedURLOptions) (url string, err error) {
	dirName, fileName := path.Split(filepath)
	defer func() {
		if fs.hook != nil {
			fs.hook(ctx, SignedURL, dirName, fileName, fs.storageType, err)
		}
	}()
	return fs.signedURL(ctx, dirName, fileName, opts)
}

func (fs *BlobFS) signedURL(ctx context.Context, dirName, fileName string, opts *blob.SignedURLOptions) (string, error) {
	bucket, err := fs.openBucket(ctx, dirName)
	if err != nil {
		return "", err
	}
	defer bucket.Close()

	return bucket.SignedURL(ctx, fileName, opts)
}

func (fs *BlobFS) OpenBucket(ctx context.Context, dir string) (bucket *blob.Bucket, err error) {
	defer func() {
		if fs.hook != nil {
			fs.hook(ctx, OpenBucket, dir, "", fs.storageType, err)
		}
	}()
	return fs.openBucket(ctx, dir)
}

func (fs *BlobFS) openBucket(ctx context.Context, dir string) (*blob.Bucket, error) {
	var bucket *blob.Bucket

	u, err := url.Parse(fs.storageURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == s3blob.Scheme {
		sess, rest, err := gcaws.NewSessionFromURLParams(u.Query())
		if err != nil {
			return nil, fmt.Errorf("open bucket %v: %v", u, err)
		}
		configProvider := &gcaws.ConfigOverrider{
			Base: sess,
		}
		overrideCfg, err := gcaws.ConfigFromURLParams(rest)
		if err != nil {
			return nil, fmt.Errorf("open bucket %v: %v", u, err)
		}

		var insecureTLS bool
		if overrideCfg.Endpoint != nil {
			u, err := url.Parse(*overrideCfg.Endpoint)
			if err != nil {
				return nil, err
			}
			// use InsecureSkipVerify, if IP address is used for baseURL host
			if ip := net.ParseIP(u.Hostname()); ip != nil && u.Scheme == "https" {
				insecureTLS = true
			}
		}
		if err := configureTLS(overrideCfg, fs.CACert, insecureTLS); err != nil {
			return nil, err
		}

		configProvider.Configs = append(configProvider.Configs, overrideCfg)

		bucket, err = s3blob.OpenBucket(ctx, configProvider, u.Host, nil)
		if err != nil {
			return nil, err
		}
	} else {
		bucket, err = blob.OpenBucket(ctx, fs.storageURL)
		if err != nil {
			return nil, err
		}
	}

	prefix := strings.Trim(path.Join(fs.prefix, dir), "/") + "/"
	if prefix == string(os.PathSeparator) {
		return bucket, nil
	}
	return blob.PrefixedBucket(bucket, prefix), nil
}

func extractStorageType(storageURL string) string {
	u, err := url.Parse(storageURL)
	if err != nil || u.Scheme == "" {
		return storageURL
	}
	return u.Scheme
}

func configureTLS(config *aws.Config, caCert []byte, insecureTLS bool) error {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecureTLS,
	}
	if caCert != nil {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			return fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}
	defaultHTTPTransport := http.DefaultTransport.(*http.Transport).Clone()
	defaultHTTPTransport.TLSClientConfig = tlsConfig

	config.HTTPClient = &http.Client{
		Transport: defaultHTTPTransport,
	}
	return nil
}

func CreateBucketURL(bucketURL, endpoint, region string) string {
	u, err := url.Parse(bucketURL)
	if err != nil {
		panic(errors.Wrapf(err, "invalid bucket URL %s", bucketURL))
	}

	if u.Scheme == s3blob.Scheme {
		values := u.Query()
		values.Set("s3ForcePathStyle", "true")
		if endpoint != "" {
			values.Set("endpoint", endpoint)
		}
		if region != "" {
			values.Set("region", region)
		}
		u.RawQuery = values.Encode()
	}
	return u.String()
}
