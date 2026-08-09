// Package temporal implements AgentOS workflows and activities on Temporal.
package temporal

import (
	"fmt"
)

type artifactStoreValidationErrors struct {
	backendRequired   error
	backendUnknown    error
	localRootRequired error
	s3BucketRequired  error
	s3RegionRequired  error
	s3AccessRequired  error
	s3SecretRequired  error
}

func validateArtifactStoreConfig(cfg *ArtifactStoreConfig, errs *artifactStoreValidationErrors) error {
	switch cfg.Backend {
	case ArtifactStoreBackendLocal:
		if cfg.Local.Root == "" {
			return errs.localRootRequired
		}
	case ArtifactStoreBackendS3:
		if cfg.S3.Bucket == "" {
			return errs.s3BucketRequired
		}

		if cfg.S3.Region == "" {
			return errs.s3RegionRequired
		}

		if cfg.S3.AccessKeyID == "" {
			return errs.s3AccessRequired
		}

		if cfg.S3.SecretAccessKey == "" {
			return errs.s3SecretRequired
		}
	case "":
		return errs.backendRequired
	default:
		return fmt.Errorf("%w: %s", errs.backendUnknown, cfg.Backend)
	}

	return nil
}
