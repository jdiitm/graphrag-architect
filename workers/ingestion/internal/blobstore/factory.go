package blobstore

import (
	"fmt"
	"log"
)

func NewBlobStoreFromEnv(storeType, bucket, region string) (BlobStore, error) {
	switch storeType {
	case "s3":
		if bucket == "" {
			return nil, fmt.Errorf("blobstore: s3 requires a non-empty bucket name")
		}
		log.Printf("blobstore: using S3 backend bucket=%s region=%s", bucket, region)
		return newS3BlobStoreFromConfig(bucket, region)
	default:
		log.Println("blobstore: using in-memory backend (development only)")
		return NewInMemoryBlobStore(), nil
	}
}

func newS3BlobStoreFromConfig(bucket, region string) (BlobStore, error) {
	client, err := newAWSS3Client(region)
	if err != nil {
		return nil, fmt.Errorf("blobstore: create s3 client: %w", err)
	}
	return NewS3BlobStore(client, bucket), nil
}
