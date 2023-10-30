package osclient

import "errors"

// Errors
var (
	ErrKeyValueConfigRequired = errors.New("ms-object-store-client: config required")
	ErrInvalidBucketName      = errors.New("ms-object-store-client: invalid bucket name")
	ErrInvalidKey             = errors.New("ms-object-store-client: invalid key")
	ErrKeyCastingFailed       = errors.New("ms-object-store-client: casting failed with invalid value for key")
	ErrBucketNotFound         = errors.New("ms-object-store-client: bucket not found")
	ErrBadBucket              = errors.New("ms-object-store-client: bucket not valid key-value store")
	ErrKeyNotFound            = errors.New("ms-object-store-client: key not found")
	ErrKeyAlreadyExists       = errors.New("ms-object-store-client: key not found")
	ErrKeyDeleted             = errors.New("ms-object-store-client: key was deleted")
	ErrHistoryToLarge         = errors.New("ms-object-store-client: history limited to a max of 64")
	ErrNoKeysFound            = errors.New("ms-object-store-client: no keys found")
)
