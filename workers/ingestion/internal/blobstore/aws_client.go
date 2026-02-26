package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type awsS3Wrapper struct {
	client *s3.Client
}

func newAWSS3Client(region string) (ObjectStorageClient, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}
	return &awsS3Wrapper{client: s3.NewFromConfig(cfg)}, nil
}

func (w *awsS3Wrapper) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

func (w *awsS3Wrapper) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := w.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (w *awsS3Wrapper) HeadObject(ctx context.Context, bucket, key string) (bool, error) {
	_, err := w.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
