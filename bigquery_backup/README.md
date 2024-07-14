[[_TOC_]]

# What the BigQuery Backup Cloud Function is User For

This code defines a function called bigQueryBackup that handles HTTP requests to back up a BigQuery table to Google Cloud Storage.

The purpose of `bigQueryBackup` is to initiate and perform a backup of a BigQuery table to Cloud Storage. It takes in an HTTP request containing parameters for the backup. It validates the parameters, checks that the BigQuery and Cloud Storage resources exist, executes the backup job, logs information, and returns HTTP responses indicating success or failure.

The input it takes is an HTTP POST request containing a JSON body with the backup parameters: `"dataset_name", "table_name", "storage_bucket", "destination_format", and "compression_type"` which are for the source BigQuery dataset name, table name, destination Cloud Storage bucket, backup file format, and compression format. The only required parameters are `"dataset_name", "table_name", and "storage_bucket"`. The `"destination_format"` defaults to `"AVRO"` and the `"compression_type"` defaults to `"SNAPPY"` if no value is provided.

The outputs it produces are HTTP responses indicating whether the backup succeeded or failed, as well as log messages written to Stackdriver Logging.

The logic first decodes the JSON body from the request into a struct containing the parameters. It validates that all required parameters are present. It sets the backup parameters on a backupParams struct for later use.

It then validates that the specified BigQuery dataset and table exist by calling the BigQuery API to get their metadata. It also checks that the Cloud Storage bucket exists.

It validates and sets the proper compression based on the requested backup file format.

Finally, it calls the BigQuery API to start a backup job to copy the table to a file in Cloud Storage in the requested format and compression.

The code logs informational and error messages to Stackdriver Logging throughout the process.

It returns HTTP 200 OK if the backup succeeded, or HTTP 500 Internal Server Error if any validation failed or the backup job encountered an error.

So in summary, this code handles initiating and performing BigQuery table backups, validates parameters and resources, executes the backup, logs information, and returns success/failure HTTP responses.

# BigQuery Backup Cloud Function Local Development

## Install tools and dependencies

- Install [golang](https://go.dev/doc/install) if you don't have it already for the platform of your choosing.
  > **NOTE** You may need to restart your terminal after installing golang to get the `go` command to work.
- Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) if you don't have it already for the platform of your choosing.
  > **NOTE** You may need to restart your terminal after installing the Google Cloud SDK to get the `gcloud` command to work.

## Running the function emulator locally

- Once you have golang installed, open up a terminal tab in the `bigquery_backup\cmd` directory and run to download the dependencies needed to run the function emulation.

  ```
  go mod download
  ```

- Now we need to login authenticate with gcloud sdk to use our client library web service calls. Run the following the prompt to login.
  ```bash
  gcloud auth application-default login
  ```
- Next run the following commands to set some environment variables, replacing `<YOUR-PROJECT-ID>` with the google project Id you are running the function, ie `super-secret-project-12345`.
  ```bash
  export PROJECT_ID=<your-project-id>
  export FUNCTION_TARGET=BigQueryBackup
  ```
- Then run `go run main.go`
- If you are running on a mac, you may get a popup similiar to that below. If you do, enter your password and click `Allow` to allow the function emulator to run.

  ![image](./readme_images/main_popup_allow.png)

- You should see something similar to the following output in your terminal.

  ```bash
  Biguery Backup starting up on PORT 8080
  GCP_PROJECT: <YOUR-PROJECT-ID>

  Open a new tab in your terminal and run, the following command to test the function. The dataset_name and table_name should match the dataset and table you wish to backup. The storage_bucket should match the bucket you wish to store the backup in. The destination_format and the compression_format should match the format you wish to store the backup in, these permissions are optional.

  ```

  To test the function open and terminal tab and run the following command, replacing your, `"dataset_name"`, `"table_name"`, and `"storage_bucket"` with those of your own.

  ```bash
  curl -H 'Content-Type: application/json' \

      -d '{ "dataset_name":"<YOUR-DATASET-NAME>","table_name":"<YOUR-TABLE-NAME>", "storage_bucket": "<YOUR-STORAGE-BUCKET>", "destination_format":"AVRO", "compression_type":"SNAPPY" }' \
      -X POST \
      http://localhost:8080
  ```

  > **NOTE** If you wish to use a port other than port 8080, you can set the `PORT` environment variable to the port of your choosing. For example, `export PORT=8081` will run the function emulator on port 8081. You will need to update the port and then rerun the `go run main.go` command.
