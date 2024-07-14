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

var lc *logging.Client
var bc *bigquery.Client
var lcOnce sync.Once
var bcOnce sync.Once

func init() {
	functions.HTTP("BigQueryBackup", bigQueryBackup)
}

func bigQueryBackup(w http.ResponseWriter, r *http.Request) {

	backupParams := backupParams{}
	err := backupParams.setProjectID()
	if err != nil {
		return
	}
	ctx := context.Background()

	err = backupParams.setLogger(ctx)
	if err != nil {
		log.Fatalf("Failed to set logger: %v", err)
		return
	}

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
		err = logError("Problem validating destination format")
		if err != nil {
			return
		}
		return
	}

	if ok, err := backupParams.backupBigQueryTable(ctx); !ok {
		if err != nil {
			err = logError("Problem backing up BigQuery table")
			if err != nil {
				return
			}
		}
	}
}

func (bp *backupParams) backupBigQueryTable(ctx context.Context) (bool, error) {
	extractor := setupExtractor(bp)

	err := logInfo(fmt.Sprintf("Starting backup of table %s.%s to cloud storage", bp.sourceDatasetID, bp.backupTableID))
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

func (bp *backupParams) waitForJob(ctx context.Context, job *bigquery.Job) (bool, error) {
	status, err := job.Wait(ctx)
	if err != nil {
		err := logError(fmt.Sprintf("Error waiting for backup of table %s.%s to cloud storage: %v", bp.sourceDatasetID, bp.backupTableID, err))
		if err != nil {
			return false, err
		}
		return true, err
	}
	if status.Err() != nil {
		err := logError(fmt.Sprintf("Error backing up table %s.%s to cloud storage: %v", bp.sourceDatasetID, bp.backupTableID, status.Err()))
		if err != nil {
			return false, err
		}
		return true, err
	}
	err = logInfo(fmt.Sprintf("Backup of table %s.%s completed successfully", bp.sourceDatasetID, bp.backupTableID))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (bp *backupParams) runExtractor(ctx context.Context, extractor *bigquery.Extractor) (*bigquery.Job, error) {
	job, err := extractor.Run(ctx)
	if err != nil {
		err = logError(fmt.Sprintf("Error starting backup of table %s.%s to cloud storage: %v", bp.sourceDatasetID, bp.backupTableID, err))
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	err = logInfo(fmt.Sprintf("Backup of table %s.%s started successfully, jobID: %s", bp.sourceDatasetID, bp.backupTableID, job.ID()))
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Setup functions
func (bp *backupParams) setProjectID() error {
	projectID := os.Getenv("GCP_PROJECT")
	if projectID == "" || strings.TrimSpace(projectID) == "" {
		return errors.New("GCP_PROJECT environment variable is not set")
	} else {
		bp.projectID = projectID
	}
	return nil
}

func (bp *backupParams) setLogger(ctx context.Context) error {
	var err error
	lcOnce.Do(func() {
		lc, err = logging.NewClient(ctx, bp.projectID)
		if err != nil {
			return
		}
	})
	defer lc.Close()
	return nil
}

func (bp *backupParams) setBigQueryClient(ctx context.Context) error {
	var err error
	bcOnce.Do(func() {
		bc, err = bigquery.NewClient(ctx, bp.projectID)
		if err != nil {
			err = logError(fmt.Sprintf("Failed to create new BigQuery client: %v", err))
			if err != nil {
				return
			}
		}
	})
	defer bc.Close()
	return nil
}

func (bp *backupParams) handleSetup(r *http.Request) bool {
	pb, err := decodePostBody(r)
	if err != nil {
		_ = logError(fmt.Sprintf("Failed to decode POST body: %v", err))
		return true
	}

	if ok, err := checkPostBody(&pb); !ok || err != nil {
		_ = logError(fmt.Sprintf("Invalid POST body: %v", err))
		return true
	}

	bp.setBackupParams(pb)
	p := fmt.Sprintf("Backup params: %s, %s, %s, %s", bp.projectID, bp.sourceDatasetID, bp.backupTableID, bp.storageBucket)
	err = logInfo(p)
	if err != nil {
		_ = logError(fmt.Sprintf("Failed to set backup params: %v", err))
		return true
	}
	return false
}

func checkPostBody(pb *postBodyParams) (bool, error) {
	if pb.DatasetName == "" {
		err := logError("Missing DatasetName in Post Body")
		return false, err
	} else if pb.TableName == "" {
		err := logError("Missing TableName in Post Body")
		return false, err
	} else if pb.StorageBucket == "" {
		err := logError("Missing StorageBucket in Post Body")
		return false, err
	}
	return true, nil
}

func decodePostBody(r *http.Request) (postBodyParams, error) {
	var pb postBodyParams
	if err := json.NewDecoder(r.Body).Decode(&pb); err != nil {
		return pb, err
	}
	return pb, nil
}

func (bp *backupParams) setBackupParams(pb postBodyParams) {
	bp.sourceDatasetID = pb.DatasetName
	bp.backupTableID = pb.TableName
	bp.storageBucket = pb.StorageBucket
	bp.destinationFormat = pb.Format
	bp.compressionType = pb.Compression
}

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
func (bp *backupParams) validateParams(ctx context.Context) bool {
	validDataset, err := bp.validateDataset(ctx)
	if err != nil || !validDataset {
		_ = logError("Dataset does not exist or is not valid")
		return true
	}

	validTable, err := bp.validateTable(ctx)
	if err != nil || !validTable {
		_ = logError(fmt.Sprintf("Table does not exist or is not valid: %v", err))
		return true
	}

	if ok, err := bp.validateStorageBucket(ctx); !ok || err != nil {
		_ = logError("Problem validating storage bucket")
		return true
	}
	return false
}

func (bp *backupParams) validateDataset(ctx context.Context) (bool, error) {
	ds := bc.Dataset(bp.sourceDatasetID)
	md, err := ds.Metadata(ctx)
	defer bc.Close()
	if err != nil {
		return false, err
	}
	if md.FullID == bp.projectID+":"+bp.sourceDatasetID {
		return true, nil
	}
	return false, nil
}

func (bp *backupParams) validateTable(ctx context.Context) (bool, error) {
	md, err := bc.Dataset(bp.sourceDatasetID).Table(bp.backupTableID).Metadata(ctx)
	fmt.Println(md.FullID)
	defer bc.Close()
	if err != nil {
		return false, err
	}
	if md.FullID == bp.projectID+":"+bp.sourceDatasetID+"."+bp.backupTableID {
		return true, nil
	}
	return false, nil
}

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

// logInfo logs an informational message to the Cloud Logging client.
func logInfo(msg string) error {
	lc.Logger("bigquery-backup").Log(logging.Entry{Payload: msg, Severity: logging.Info})
	return nil
}

// logError logs an error message to the Cloud Logging client.
func logError(msg string) error {
	lc.Logger("bigquery-backup").Log(logging.Entry{Payload: msg, Severity: logging.Error})
	return nil
}

// Formatting functions

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

func (bp *backupParams) setCSVAndJSONCompression() error {
	bp.compressionType = gzipCompression
	lv := fmt.Sprintf("Backup format: %s, Backup compression: %s", bp.destinationFormat, bp.compressionType)
	err := logInfo(lv)
	if err != nil {
		return err
	}
	return nil
}

func (bp *backupParams) setAvroParquetCompression() error {
	if bp.compressionType == "" {
		bp.compressionType = snappyCompression
		lv := fmt.Sprintf("BigQuery Table will be Backup format: %s, Backup compression: %s", bp.destinationFormat, bp.compressionType)
		err := logInfo(lv)
		if err != nil {
			return err
		} else if bp.compressionType == deflateCompression || bp.compressionType == snappyCompression {
			lv := fmt.Sprintf("Backup format: %s, Backup compression: %s", bp.destinationFormat, bp.compressionType)
			err := logInfo(lv)
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
