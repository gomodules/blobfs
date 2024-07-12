package blobfs

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
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

type BlobFS struct {
	storageURL string
	prefix     string

	CACert      []byte
	InsecureTLS bool
	Endpoint    string
	Region      string
}

func New(storageURL string, prefix ...string) *BlobFS {
	var bucketPrefix string
	if len(prefix) > 0 {
		bucketPrefix = prefix[0]
	}

	return &BlobFS{
		storageURL: storageURL,
		prefix:     bucketPrefix,
	}
}

var _ Interface = (*BlobFS)(nil)

func NewInMemoryFS() Interface {
	return New("mem://")
}

func NewOsFs() Interface {
	return New("file:///")
}

func (fs *BlobFS) WriteFile(ctx context.Context, filepath string, data []byte) error {
	dir, filename := path.Split(filepath)
	bucket, err := fs.OpenBucket(ctx, path.Join(fs.prefix, dir))
	if err != nil {
		return err
	}
	defer bucket.Close()

	w, err := bucket.NewWriter(ctx, filename, nil)
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

func (fs *BlobFS) ReadFile(ctx context.Context, filepath string) ([]byte, error) {
	dir, filename := path.Split(filepath)
	bucket, err := fs.OpenBucket(ctx, path.Join(fs.prefix, dir))
	if err != nil {
		return nil, err
	}
	defer bucket.Close()
	// Open the key "foo.txt" for reading with the default options.
	r, err := bucket.NewReader(ctx, filename, nil)
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

func (fs *BlobFS) DeleteFile(ctx context.Context, filepath string) error {
	dir, filename := path.Split(filepath)
	bucket, err := fs.OpenBucket(ctx, path.Join(fs.prefix, dir))
	if err != nil {
		return err
	}
	defer bucket.Close()

	return bucket.Delete(context.TODO(), filename)
}

func (fs *BlobFS) Exists(ctx context.Context, filepath string) (bool, error) {
	dir, filename := path.Split(filepath)
	bucket, err := fs.OpenBucket(ctx, path.Join(fs.prefix, dir))
	if err != nil {
		return false, err
	}
	defer bucket.Close()

	return bucket.Exists(context.TODO(), filename)
}

func (fs *BlobFS) SignedURL(ctx context.Context, filepath string, opts *blob.SignedURLOptions) (string, error) {
	dir, filename := path.Split(filepath)
	bucket, err := fs.OpenBucket(ctx, path.Join(fs.prefix, dir))
	if err != nil {
		return "", err
	}
	defer bucket.Close()

	return bucket.SignedURL(ctx, filename, opts)
}

func (fs *BlobFS) OpenBucket(ctx context.Context, dir string) (*blob.Bucket, error) {
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
		if err := configureTLS(overrideCfg, fs.CACert, fs.InsecureTLS); err != nil {
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

	prefix := strings.Trim(dir, "/") + "/"
	if prefix == string(os.PathSeparator) {
		return bucket, nil
	}
	return blob.PrefixedBucket(bucket, prefix), nil
}

func (fs *BlobFS) getS3Session() (*session.Session, error) {
	config := &aws.Config{
		Region:                        aws.String(fs.Region),
		CredentialsChainVerboseErrors: aws.Bool(true),
		Endpoint:                      aws.String(fs.Endpoint),
		S3ForcePathStyle:              aws.Bool(true),
	}
	if err := configureTLS(config, fs.CACert, fs.InsecureTLS); err != nil {
		return nil, err
	}

	sess := session.Must(session.NewSession())
	config.WithCredentials(credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvProvider{},
		&credentials.SharedCredentialsProvider{},
		// Required for IRSA
		stscreds.NewWebIdentityRoleProviderWithOptions(
			sts.New(sess),
			os.Getenv(awsRoleArn),
			"",
			stscreds.FetchTokenPath(os.Getenv(awsWebIdentityTokenFile)),
		),
		&ec2rolecreds.EC2RoleProvider{
			Client: ec2metadata.New(sess),
		},
	}))
	return session.NewSession(config)
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
