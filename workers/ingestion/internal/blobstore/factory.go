package blobstore

import "log"

func NewBlobStoreFromEnv(storeType, bucket, region string) BlobStore {
	switch storeType {
	case "s3":
		log.Printf("blobstore: using S3 backend bucket=%s region=%s", bucket, region)
		return newS3BlobStoreFromConfig(bucket, region)
	default:
		log.Println("blobstore: using in-memory backend (development only)")
		return NewInMemoryBlobStore()
	}
}

func newS3BlobStoreFromConfig(bucket, region string) BlobStore {
	client := newAWSS3Client(region)
	return NewS3BlobStore(client, bucket)
}
