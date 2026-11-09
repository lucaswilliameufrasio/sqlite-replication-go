// Package storages3 provides an S3-compatible immutable object store.
// It is independent from WAL capture and the replication core.
package storages3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	copyBufferSize     = 1024 * 1024
	multipartThreshold = 8 * 1024 * 1024
	minimumPartSize    = 8 * 1024 * 1024
	maximumParts       = 10_000
	maximumObjectSize  = int64(5 * 1024 * 1024 * 1024 * 1024)
)

type ErrorKind uint8

const (
	ErrorConflict ErrorKind = iota + 1
	ErrorNotFound
	ErrorRetryable
	ErrorPermanent
	ErrorCanceled
)

const ErrorNone ErrorKind = 0

// StorageError deliberately omits provider response bodies, endpoints, and keys.
type StorageError struct {
	Operation string
	Kind      ErrorKind
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("S3 %s failed (class %d)", e.Operation, e.Kind)
}

type Capabilities struct {
	ConditionalPut               bool
	ConditionalMultipartComplete bool
	ByteRanges                   bool
	MultipartUpload              bool
	DeleteBatchLimit             int
	MaxObjectSizeBytes           int64
}

// Metrics records low-cardinality observations without object keys or metadata.
type Metrics interface {
	ObserveStorage(operation string, duration time.Duration, bytes int64, class ErrorKind)
}

type PutReceipt struct {
	Size   int64
	SHA256 string
}

type ObjectDescriptor struct {
	Key    string
	Size   int64
	SHA256 string
}

// ReconcileReport is observational only: reconciliation never mutates remote
// objects or advances a caller's acknowledgement watermark.
type ReconcileReport struct {
	Missing    []string
	Divergent  []string
	Unexpected []string
}

type Store struct {
	client  *s3.Client
	bucket  string
	metrics Metrics
}

func New(client *s3.Client, bucket string) (*Store, error) {
	return NewWithMetrics(client, bucket, nil)
}

func NewWithMetrics(client *s3.Client, bucket string, metrics Metrics) (*Store, error) {
	if client == nil {
		return nil, &StorageError{Operation: "configure", Kind: ErrorPermanent}
	}
	if strings.TrimSpace(bucket) == "" {
		return nil, &StorageError{Operation: "configure", Kind: ErrorPermanent}
	}
	return &Store{client: client, bucket: bucket, metrics: metrics}, nil
}

// Capabilities reports the protocol features this adapter requests. It is not
// endpoint discovery; the deployment must verify those semantics for its S3 service.
func (s *Store) Capabilities() Capabilities {
	return Capabilities{
		ConditionalPut:               true,
		ConditionalMultipartComplete: true,
		ByteRanges:                   true,
		MultipartUpload:              true,
		DeleteBatchLimit:             1000,
		MaxObjectSizeBytes:           maximumObjectSize,
	}
}

// PutImmutable stages input on disk so the checksum and size are known before
// issuing a conditional PUT. Memory use is bounded independently of object size.
func (s *Store) PutImmutable(ctx context.Context, key string, input io.Reader) (PutReceipt, error) {
	return s.PutImmutableWithMetadata(ctx, key, input, nil)
}

func (s *Store) PutImmutableWithMetadata(ctx context.Context, key string, input io.Reader, userMetadata map[string]string) (receipt PutReceipt, returnErr error) {
	started := time.Now()
	defer func() { s.observe("put", started, receipt.Size, returnErr) }()
	if err := validateKey(key); err != nil {
		return PutReceipt{}, err
	}
	metadata, err := normalizeMetadata(userMetadata)
	if err != nil {
		return PutReceipt{}, err
	}
	stage, err := os.CreateTemp("", "sqlite-replication-s3-*")
	if err != nil {
		return PutReceipt{}, storageError("stage", ErrorPermanent, err)
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	defer stage.Close()

	hash := sha256.New()
	size, err := io.CopyBuffer(io.MultiWriter(stage, hash), contextReader{ctx: ctx, reader: input}, make([]byte, copyBufferSize))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return PutReceipt{}, storageError("stage", ErrorCanceled, err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return PutReceipt{}, storageError("stage", ErrorRetryable, err)
		}
		return PutReceipt{}, storageError("stage", ErrorPermanent, err)
	}
	if size > maximumObjectSize {
		return PutReceipt{}, storageError("put", ErrorPermanent, errors.New("object exceeds S3 limit"))
	}
	if err := stage.Sync(); err != nil {
		return PutReceipt{}, storageError("stage", ErrorPermanent, err)
	}
	receipt = PutReceipt{Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}
	if err := s.putStaged(ctx, key, stage, receipt, metadata); err != nil {
		matches, verifyErr := s.verifyExisting(ctx, key, receipt, metadata)
		if verifyErr == nil && matches {
			return receipt, nil
		}
		if verifyErr == nil && !matches {
			return PutReceipt{}, storageError("put", ErrorConflict, err)
		}
		var verifyStorageError *StorageError
		if errors.As(verifyErr, &verifyStorageError) && verifyStorageError.Kind != ErrorNotFound {
			return PutReceipt{}, verifyErr
		}
		return PutReceipt{}, err
	}
	return receipt, nil
}

func (s *Store) putStaged(ctx context.Context, key string, stage *os.File, receipt PutReceipt, userMetadata map[string]string) error {
	metadata := make(map[string]string, len(userMetadata)+2)
	for key, value := range userMetadata {
		metadata[key] = value
	}
	metadata["content-sha256"] = receipt.SHA256
	metadata["content-length"] = fmt.Sprint(receipt.Size)
	if receipt.Size < multipartThreshold {
		if _, err := stage.Seek(0, io.SeekStart); err != nil {
			return storageError("put", ErrorPermanent, err)
		}
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          stage,
			ContentLength: aws.Int64(receipt.Size),
			IfNoneMatch:   aws.String("*"),
			Metadata:      metadata,
		})
		if err != nil {
			return classifyError("put", err)
		}
		return nil
	}
	return s.putMultipart(ctx, key, stage, receipt, metadata)
}

func (s *Store) putMultipart(ctx context.Context, key string, stage *os.File, receipt PutReceipt, metadata map[string]string) error {
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Metadata: metadata,
	})
	if err != nil {
		return classifyError("multipart-create", err)
	}
	if created.UploadId == nil || *created.UploadId == "" {
		return storageError("multipart-create", ErrorPermanent, errors.New("missing upload id"))
	}
	uploadID := *created.UploadId
	completedParts := make([]types.CompletedPart, 0)
	partSize := int64(minimumPartSize)
	if needed := (receipt.Size + int64(maximumParts) - 1) / int64(maximumParts); needed > partSize {
		partSize = needed
	}
	for offset, partNumber := int64(0), int32(1); offset < receipt.Size; partNumber++ {
		length := partSize
		if remaining := receipt.Size - offset; remaining < length {
			length = remaining
		}
		body := io.NewSectionReader(stage, offset, length)
		uploaded, uploadErr := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          body,
			ContentLength: aws.Int64(length),
		})
		if uploadErr != nil {
			s.abortMultipart(ctx, key, uploadID)
			return classifyError("multipart-part", uploadErr)
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag: uploaded.ETag, PartNumber: aws.Int32(partNumber),
		})
		offset += length
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		IfNoneMatch:     aws.String("*"),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err != nil {
		s.abortMultipart(ctx, key, uploadID)
		return classifyError("multipart-complete", err)
	}
	return nil
}

func (s *Store) abortMultipart(ctx context.Context, key, uploadID string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_, _ = s.client.AbortMultipartUpload(cleanupCtx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
}

func (s *Store) verifyExisting(ctx context.Context, key string, expected PutReceipt, userMetadata map[string]string) (bool, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		return false, classifyError("verify", err)
	}
	defer object.Body.Close()
	if object.ContentLength == nil || *object.ContentLength != expected.Size {
		return false, nil
	}
	if value, ok := object.Metadata["content-sha256"]; ok && !strings.EqualFold(value, expected.SHA256) {
		return false, nil
	}
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, object.Body, make([]byte, copyBufferSize)); err != nil {
		return false, storageError("verify", ErrorRetryable, err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
		return false, nil
	}
	for key, value := range userMetadata {
		remoteValue, ok := object.Metadata[key]
		if !ok || remoteValue != value {
			return false, nil
		}
	}
	return true, nil
}

func normalizeMetadata(metadata map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || strings.ContainsAny(key, " \t\r\n") || key == "content-sha256" || key == "content-length" {
			return nil, storageError("metadata", ErrorPermanent, errors.New("invalid or reserved metadata key"))
		}
		if _, exists := normalized[key]; exists {
			return nil, storageError("metadata", ErrorPermanent, errors.New("duplicate metadata key"))
		}
		normalized[key] = value
	}
	return normalized, nil
}

func (s *Store) Get(ctx context.Context, key string, byteRange string) (output *s3.GetObjectOutput, returnErr error) {
	started := time.Now()
	defer func() {
		var size int64
		if output != nil && output.ContentLength != nil {
			size = *output.ContentLength
		}
		s.observe("get", started, size, returnErr)
	}()
	if err := validateKey(key); err != nil {
		return nil, err
	}
	input := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if byteRange != "" {
		input.Range = aws.String(byteRange)
	}
	output, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, classifyError("get", err)
	}
	return output, nil
}

func (s *Store) Head(ctx context.Context, key string) (output *s3.HeadObjectOutput, returnErr error) {
	started := time.Now()
	defer func() { s.observe("head", started, 0, returnErr) }()
	if err := validateKey(key); err != nil {
		return nil, err
	}
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, classifyError("head", err)
	}
	return output, nil
}

func (s *Store) ListPage(ctx context.Context, prefix string, cursor *string, pageSize int32) (output *s3.ListObjectsV2Output, returnErr error) {
	started := time.Now()
	defer func() { s.observe("list", started, 0, returnErr) }()
	if pageSize <= 0 || pageSize > 1000 {
		return nil, storageError("list", ErrorPermanent, errors.New("invalid page size"))
	}
	output, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
		ContinuationToken: cursor, MaxKeys: aws.Int32(pageSize),
	})
	if err != nil {
		return nil, classifyError("list", err)
	}
	return output, nil
}

func (s *Store) DeleteBatch(ctx context.Context, keys []string) (returnErr error) {
	started := time.Now()
	defer func() { s.observe("delete", started, 0, returnErr) }()
	if len(keys) > 1000 {
		return storageError("delete", ErrorPermanent, errors.New("batch exceeds 1000 keys"))
	}
	if len(keys) == 0 {
		return nil
	}
	objects := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return err
		}
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
	}
	output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return classifyError("delete", err)
	}
	if len(output.Errors) != 0 {
		return storageError("delete", ErrorRetryable, errors.New("one or more objects failed"))
	}
	return nil
}

// Reconcile compares an expected local immutable-object set with every remote
// object under prefix. Matching objects are verified from bytes, not ETag or
// user metadata. All list cursors are consumed; no remote object is deleted.
func (s *Store) Reconcile(ctx context.Context, prefix string, expected []ObjectDescriptor) (report ReconcileReport, returnErr error) {
	started := time.Now()
	defer func() { s.observe("reconcile", started, 0, returnErr) }()

	byKey := make(map[string]ObjectDescriptor, len(expected))
	for _, descriptor := range expected {
		if err := validateKey(descriptor.Key); err != nil {
			return ReconcileReport{}, err
		}
		decodedHash, decodeErr := hex.DecodeString(descriptor.SHA256)
		if !strings.HasPrefix(descriptor.Key, prefix) || descriptor.Size < 0 || decodeErr != nil || len(decodedHash) != sha256.Size {
			return ReconcileReport{}, storageError("reconcile", ErrorPermanent, errors.New("invalid expected object descriptor"))
		}
		descriptor.SHA256 = strings.ToLower(descriptor.SHA256)
		if _, exists := byKey[descriptor.Key]; exists {
			return ReconcileReport{}, storageError("reconcile", ErrorPermanent, errors.New("duplicate expected object key"))
		}
		byKey[descriptor.Key] = descriptor
	}

	seenRemote := make(map[string]struct{})
	seenCursors := make(map[string]struct{})
	var cursor *string
	for {
		page, err := s.ListPage(ctx, prefix, cursor, 1000)
		if err != nil {
			return ReconcileReport{}, err
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				return ReconcileReport{}, storageError("reconcile", ErrorPermanent, errors.New("list returned an object without a key"))
			}
			key := *object.Key
			if _, duplicate := seenRemote[key]; duplicate {
				continue
			}
			seenRemote[key] = struct{}{}
			expectedObject, isExpected := byKey[key]
			if !isExpected {
				report.Unexpected = append(report.Unexpected, key)
				continue
			}
			matches, err := s.verifyExisting(ctx, key, PutReceipt{Size: expectedObject.Size, SHA256: strings.ToLower(expectedObject.SHA256)}, nil)
			if err != nil {
				return ReconcileReport{}, err
			}
			if !matches {
				report.Divergent = append(report.Divergent, key)
			}
		}

		nextCursor := page.NextContinuationToken
		if nextCursor == nil || *nextCursor == "" {
			if aws.ToBool(page.IsTruncated) {
				return ReconcileReport{}, storageError("reconcile", ErrorPermanent, errors.New("truncated list page has no continuation token"))
			}
			break
		}
		if _, repeated := seenCursors[*nextCursor]; repeated {
			return ReconcileReport{}, storageError("reconcile", ErrorPermanent, errors.New("list returned a repeated continuation token"))
		}
		seenCursors[*nextCursor] = struct{}{}
		cursor = nextCursor
	}

	for key := range byKey {
		if _, exists := seenRemote[key]; !exists {
			report.Missing = append(report.Missing, key)
		}
	}
	sort.Strings(report.Missing)
	sort.Strings(report.Divergent)
	sort.Strings(report.Unexpected)
	return report, nil
}

func validateKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.ContainsRune(key, '\x00') {
		return storageError("validate-key", ErrorPermanent, errors.New("invalid object key"))
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func storageError(operation string, kind ErrorKind, _ error) error {
	return &StorageError{Operation: operation, Kind: kind}
}

func (s *Store) observe(operation string, started time.Time, size int64, err error) {
	if s.metrics == nil {
		return
	}
	class := ErrorNone
	if err != nil {
		var storageErr *StorageError
		if errors.As(err, &storageErr) {
			class = storageErr.Kind
		} else {
			class = ErrorPermanent
		}
	}
	s.metrics.ObserveStorage(operation, time.Since(started), size, class)
}

func classifyError(operation string, cause error) error {
	kind := ErrorPermanent
	if errors.Is(cause, context.Canceled) {
		return storageError(operation, ErrorCanceled, cause)
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return storageError(operation, ErrorRetryable, cause)
	}
	var responseError *smithyhttp.ResponseError
	if errors.As(cause, &responseError) {
		switch responseError.HTTPStatusCode() {
		case 404:
			kind = ErrorNotFound
		case 409, 412:
			kind = ErrorConflict
		case 408, 429:
			kind = ErrorRetryable
		default:
			if responseError.HTTPStatusCode() >= 500 {
				kind = ErrorRetryable
			}
		}
	}
	var apiError smithy.APIError
	if errors.As(cause, &apiError) {
		switch apiError.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict", "EntityAlreadyExists":
			kind = ErrorConflict
		}
	}
	return storageError(operation, kind, cause)
}
