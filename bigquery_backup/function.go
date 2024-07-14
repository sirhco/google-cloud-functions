package bigquerybackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/logging"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

const (
	csvFormat     = "CSV"
	jsonFormat    = "JSON"
	avroFormat    = "AVRO"
	parquetFormat = "PARQUET"

	gzipCompression    = "GZIP"
	snappyCompression  = "SNAPPY"
	zstdCompression    = "ZSTD"
	deflateCompression = "DEFLATE"
)

type backupParams struct {
	projectID         string
	sourceDatasetID   string
	backupTableID     string
	storageBucket     string
	compressionType   string
	destinationFormat string
}

type postBodyParams struct {
	DatasetName   string `json:"dataset_name"`
	TableName     string `json:"table_name"`
	StorageBucket string `json:"storage_bucket"`
	Format        string `json:"destination_format"`
	Compression   string `json:"compression_type"`
}

var bc *bigquery.Client
var bcOnce sync.Once

func init() {
	functions.HTTP("BigQueryBackup", bigQueryBackup)
}

// bigQueryBackup is an HTTP function that handles a request to back up a BigQuery table to cloud storage.
// It sets up the necessary clients, validates the input parameters, and then calls the backupBigQueryTable
// function to perform the actual backup. If the backup is successful, it returns a success response.
// If there are any errors, it logs the error and returns an error response.
func bigQueryBackup(w http.ResponseWriter, r *http.Request) {

	backupParams := backupParams{}
	err := backupParams.setProjectID()
	if err != nil {
		return
	}
	ctx := context.Background()

	err = backupParams.setBigQueryClient(ctx)
	if err != nil {
		return
	}

	hasError := backupParams.handleSetup(r)
	if hasError {
		return
	}

	hasError = backupParams.validateParams(ctx)
	if hasError {
		return
	}

	if ok, err := backupParams.checkBackupFormat(); !ok || err != nil {
		err = backupParams.logError("Problem validating destination format")
		if err != nil {
			return
		}
		return
	}

	if ok, err := backupParams.backupBigQueryTable(ctx); !ok {
		if err != nil {
			err = backupParams.logError("Problem backing up BigQuery table")
			if err != nil {
				return
			}
		}
	}
}

// backupBigQueryTable backs up the specified BigQuery table to cloud storage.
// It sets up an extractor, runs the extractor, and waits for the job to complete.
// If the backup is successful, it returns true. If there is an error, it returns false and the error.
func (bp *backupParams) backupBigQueryTable(ctx context.Context) (bool, error) {
	extractor := setupExtractor(bp)

	err := bp.logInfo(fmt.Sprintf("Starting backup of table %s.%s to cloud storage", bp.sourceDatasetID, bp.backupTableID))
	if err != nil {
		return false, err
	}

	job, err := bp.runExtractor(ctx, extractor)
	if err != nil {
		return false, err
	}

	ok, err := bp.waitForJob(ctx, job)
	if !ok {
		return false, err
	}
	return true, nil
}

// waitForJob waits for the provided BigQuery job to complete and logs the status.
// It returns true if the job completed successfully, or false if there was an error.
// If there is an error, it also returns the error.
func (bp *backupParams) waitForJob(ctx context.Context, job *bigquery.Job) (bool, error) {
	status, err := job.Wait(ctx)
	if err != nil {
		err := bp.logError(fmt.Sprintf("Error waiting for backup of table %s.%s to cloud storage: %v", bp.sourceDatasetID, bp.backupTableID, err))
		if err != nil {
			return false, err
		}
		return true, err
	}
	if status.Err() != nil {
		err := bp.logError(fmt.Sprintf("Error backing up table %s.%s to cloud storage: %v", bp.sourceDatasetID, bp.backupTableID, status.Err()))
		if err != nil {
			return false, err
		}
		return true, err
	}
	err = bp.logInfo(fmt.Sprintf("Backup of table %s.%s completed successfully", bp.sourceDatasetID, bp.backupTableID))
	if err != nil {
		return false, err
	}
	return true, nil
}

// runExtractor runs the provided BigQuery extractor and logs the start and job ID of the backup operation.
// It returns the BigQuery job that was started, or an error if there was a problem starting the job.
func (bp *backupParams) runExtractor(ctx context.Context, extractor *bigquery.Extractor) (*bigquery.Job, error) {
	job, err := extractor.Run(ctx)
	if err != nil {
		err = bp.logError(fmt.Sprintf("Error starting backup of table %s.%s to cloud storage: %v", bp.sourceDatasetID, bp.backupTableID, err))
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	err = bp.logInfo(fmt.Sprintf("Backup of table %s.%s started successfully, jobID: %s", bp.sourceDatasetID, bp.backupTableID, job.ID()))
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Setup functions

// setProjectID sets the project ID for the backup parameters based on the
// GCP_PROJECT environment variable. If the environment variable is not set or
// is empty, it returns an error.
func (bp *backupParams) setProjectID() error {
	projectID := os.Getenv("GCP_PROJECT")
	if projectID == "" || strings.TrimSpace(projectID) == "" {
		return errors.New("GCP_PROJECT environment variable is not set")
	} else {
		bp.projectID = projectID
	}
	return nil
}

// setBigQueryClient creates a new BigQuery client for the specified project ID.
// It uses the bcOnce sync.Once to ensure the client is only created once, and
// returns any error that occurred during the client creation process.
func (bp *backupParams) setBigQueryClient(ctx context.Context) error {
	var err error
	bcOnce.Do(func() {
		bc, err = bigquery.NewClient(ctx, bp.projectID)
		if err != nil {
			err = bp.logError(fmt.Sprintf("Failed to create new BigQuery client: %v", err))
			if err != nil {
				return
			}
		}
	})
	defer bc.Close()
	return nil
}

// handleSetup processes the incoming HTTP request, decodes the request body,
// validates the required fields, and sets the backup parameters based on the
// provided post body. It returns a boolean indicating whether an error occurred
// during the setup process.
func (bp *backupParams) handleSetup(r *http.Request) bool {
	pb, err := decodePostBody(r)
	if err != nil {
		_ = bp.logError(fmt.Sprintf("Failed to decode POST body: %v", err))
		return true
	}

	if ok, err := bp.checkPostBody(&pb); !ok || err != nil {
		_ = bp.logError(fmt.Sprintf("Invalid POST body: %v", err))
		return true
	}

	bp.setBackupParams(pb)
	p := fmt.Sprintf("Backup params: %s, %s, %s, %s", bp.projectID, bp.sourceDatasetID, bp.backupTableID, bp.storageBucket)
	err = bp.logInfo(p)
	if err != nil {
		_ = bp.logError(fmt.Sprintf("Failed to set backup params: %v", err))
		return true
	}
	return false
}

// checkPostBody validates the required fields in the postBodyParams struct.
// It checks that the DatasetName, TableName, and StorageBucket fields are
// not empty. If any of these fields are missing, it logs an error and
// returns false along with the error.
func (bp *backupParams) checkPostBody(pb *postBodyParams) (bool, error) {
	if pb.DatasetName == "" {
		err := bp.logError("Missing DatasetName in Post Body")
		return false, err
	} else if pb.TableName == "" {
		err := bp.logError("Missing TableName in Post Body")
		return false, err
	} else if pb.StorageBucket == "" {
		err := bp.logError("Missing StorageBucket in Post Body")
		return false, err
	}
	return true, nil
}

// decodePostBody decodes the HTTP request body into a postBodyParams struct.
// It uses json.NewDecoder to decode the request body into the provided
// postBodyParams struct, and returns the populated struct and any error
// that occurred during the decoding process.
func decodePostBody(r *http.Request) (postBodyParams, error) {
	var pb postBodyParams
	if err := json.NewDecoder(r.Body).Decode(&pb); err != nil {
		return pb, err
	}
	return pb, nil
}

// setBackupParams sets the backup parameters based on the provided postBodyParams.
// It assigns the DatasetName, TableName, StorageBucket, Format, and Compression
// from the postBodyParams to the corresponding fields in the backupParams struct.
func (bp *backupParams) setBackupParams(pb postBodyParams) {
	bp.sourceDatasetID = pb.DatasetName
	bp.backupTableID = pb.TableName
	bp.storageBucket = pb.StorageBucket
	bp.destinationFormat = pb.Format
	bp.compressionType = pb.Compression
}

// setupExtractor creates a BigQuery Extractor to export the specified table to a GCS location.
// It constructs the GCS URI for the backup file, creates a GCS reference, and configures the extractor
// with the appropriate destination format and compression type. The extractor is returned for use
// in the backup process.
func setupExtractor(bp *backupParams) *bigquery.Extractor {
	backup := fmt.Sprintf("%s.%s", bp.backupTableID, time.Now().Format("2006-01-02"))
	gcsURI := fmt.Sprintf("gs://%s/%s/%s/%s-*.%s", bp.storageBucket, bp.sourceDatasetID, backup, bp.backupTableID, strings.ToLower(bp.destinationFormat))
	gcsRef := bigquery.NewGCSReference(gcsURI)
	extractor := bc.DatasetInProject(bp.projectID, bp.sourceDatasetID).Table(bp.backupTableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	gcsRef.DestinationFormat = bigquery.DataFormat(bp.destinationFormat)
	gcsRef.Compression = bigquery.Compression(bp.compressionType)
	return extractor
}

// Validation functions

// validateParams validates that the specified dataset, table, and storage bucket exist and are accessible.
// It first checks that the dataset exists and is valid, then checks that the table exists and is valid,
// and finally checks that the storage bucket exists and is accessible. If any of these validations fail,
// the function returns true, indicating that the backup parameters are not valid. Otherwise, it returns false.
func (bp *backupParams) validateParams(ctx context.Context) bool {
	validDataset, err := bp.validateDataset(ctx)
	if err != nil || !validDataset {
		_ = bp.logError("Dataset does not exist or is not valid")
		return true
	}

	validTable, err := bp.validateTable(ctx)
	if err != nil || !validTable {
		_ = bp.logError(fmt.Sprintf("Table does not exist or is not valid: %v", err))
		return true
	}

	if ok, err := bp.validateStorageBucket(ctx); !ok || err != nil {
		_ = bp.logError("Problem validating storage bucket")
		return true
	}
	return false
}

// validateDataset validates that the specified dataset exists in the project and is accessible.
// It retrieves the metadata for the specified dataset and compares the full ID to the expected full ID
// based on the project ID and dataset ID provided in the backupParams. If the full ID matches,
// the function returns true, indicating the dataset is valid. Otherwise, it returns false.
func (bp *backupParams) validateDataset(ctx context.Context) (bool, error) {
	ds := bc.Dataset(bp.sourceDatasetID)
	md, err := ds.Metadata(ctx)
	if err != nil {
		return false, err
	}
	if md.FullID == bp.projectID+":"+bp.sourceDatasetID {
		return true, nil
	}
	return false, nil
}

// validateTable validates that the specified table exists in the source dataset and is accessible.
// It retrieves the metadata for the specified table and compares the full ID to the expected full ID
// based on the project ID and source dataset ID provided in the backupParams. If the full ID matches,
// the function returns true, indicating the table is valid. Otherwise, it returns false.
func (bp *backupParams) validateTable(ctx context.Context) (bool, error) {
	md, err := bc.Dataset(bp.sourceDatasetID).Table(bp.backupTableID).Metadata(ctx)
	fmt.Println(md.FullID)
	if err != nil {
		return false, err
	}
	if md.FullID == bp.projectID+":"+bp.sourceDatasetID+"."+bp.backupTableID {
		return true, nil
	}
	return false, nil
}

// validateStorageBucket validates that the specified storage bucket exists and is accessible.
// It creates a new storage client, retrieves the attributes of the specified bucket, and returns
// true if the bucket exists and can be accessed, or false otherwise.
func (bp *backupParams) validateStorageBucket(ctx context.Context) (bool, error) {
	c, err := storage.NewClient(ctx)
	if err != nil {
		return false, err
	}
	defer c.Close()

	bucket := c.Bucket(bp.storageBucket)
	if _, err := bucket.Attrs(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// Logging functions

// logInfo logs an informational message to the "bigquery-backup" logger.
// The message is logged with the Info severity level.
func (bp *backupParams) logInfo(msg string) error {
	ctx := context.Background()
	c, err := logging.NewClient(ctx, bp.projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()
	logName := "bigquery-backup"
	logger := c.Logger(logName).StandardLogger(logging.Info)
	logger.Println(msg)
	return nil
}

// logError logs an error message to the "bigquery-backup" logger.
// The message is logged with the Error severity level.
func (bp *backupParams) logError(msg string) error {
	ctx := context.Background()
	c, err := logging.NewClient(ctx, bp.projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()
	logName := "bigquery-backup"
	logger := c.Logger(logName).StandardLogger(logging.Error)
	logger.Println(msg)
	return nil
}

// checkBackupFormat checks the backup format specified in the backupParams and sets the appropriate compression type.
// If the backup format is CSV or JSON, it sets the compression type to gzip.
// If the backup format is Avro or Parquet, it sets the compression type to Snappy by default, or to Deflate or Snappy if specified.
// If the backup format is not recognized, it defaults to Avro format with Snappy compression.
// The function returns true if the backup format is valid, and false otherwise.
func (bp *backupParams) checkBackupFormat() (bool, error) {
	switch bp.destinationFormat {
	case csvFormat:
		err := bp.setCSVAndJSONCompression()
		if err != nil {
			return false, err
		}
		return true, nil
	case jsonFormat:
		err := bp.setCSVAndJSONCompression()
		if err != nil {
			return false, err
		}
		return true, nil
	case avroFormat:
		err := bp.setAvroParquetCompression()
		if err != nil {
			return false, err
		}
		return true, nil
	case parquetFormat:
		err := bp.setAvroParquetCompression()
		if err != nil {
			return false, err
		}
		return true, nil
	default:
		bp.destinationFormat = avroFormat
		bp.compressionType = snappyCompression
		return true, nil
	}
}

// setCSVAndJSONCompression sets the compression type for CSV and JSON backup formats to gzip.
// It logs the backup format and compression type.
func (bp *backupParams) setCSVAndJSONCompression() error {
	bp.compressionType = gzipCompression
	lv := fmt.Sprintf("Backup format: %s, Backup compression: %s", bp.destinationFormat, bp.compressionType)
	err := bp.logInfo(lv)
	if err != nil {
		return err
	}
	return nil
}

// setAvroParquetCompression sets the compression type for Avro and Parquet backup formats.
// If the compression type is not set, it defaults to Snappy compression.
// If the compression type is set to Deflate or Snappy, it logs the backup format and compression type.
// If the compression type is set to an unsupported value, it defaults to Snappy compression.
func (bp *backupParams) setAvroParquetCompression() error {
	if bp.compressionType == "" {
		bp.compressionType = snappyCompression
		lv := fmt.Sprintf("BigQuery Table will be Backup format: %s, Backup compression: %s", bp.destinationFormat, bp.compressionType)
		err := bp.logInfo(lv)
		if err != nil {
			return err
		} else if bp.compressionType == deflateCompression || bp.compressionType == snappyCompression {
			lv := fmt.Sprintf("Backup format: %s, Backup compression: %s", bp.destinationFormat, bp.compressionType)
			err := bp.logInfo(lv)
			if err != nil {
				return err
			}
			return nil
		} else {
			bp.compressionType = snappyCompression
			return nil
		}
	}
	return nil
}
