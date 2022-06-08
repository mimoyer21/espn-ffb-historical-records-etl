# README.md for espn_ffb_historical_records_etl Pipeline
Airflow DAG to run ETL process to populate a BigQuery db with historical records from the Jayhawk 
Keeper League fantasy football league. Pipeline also outputs data as a CSV delivered via email.

## Background:
The Jayhawk Keeper League has been a fantasy football league (hosted on ESPN) among friends since
2008. There is no better way to promote friendly trash talking than to surface historical league-member
performance, so that's what this pipeline does.
_Note: this Airflow instance is set up in its most basic form, hosted locally using the default SQLite 
database and SequentialExecutor, so it is certainly not meant for production use._

## Steps:
All of the processing happens within the `run_python_script` step of the dag that runs ffb_gcp.py. 
Within it, the following occurs:
* an empty CSV is created in the output folder
* for each year in the designated range, the year's final standings are written to the CSV
* the complete CSV is uploaded to Google Cloud Storage
* the data is loaded from GCS to the BigQuery table `jkl_records.jkl_standings_yearly`
Once that all happens within the `run_python_script` step, the CSV is also emailed to the designated
user in the `email_result_csv` step of the dag.

## Final DAG:
![alt text]() 

## BigQuery Output:
[Link to table in BigQuery console](https://console.cloud.google.com/bigquery?_ga=2.123119171.934828281.1654637609-1773417848.1651011285&project=fantasy-football-test-338723&ws=!1m5!1m4!4m3!1sfantasy-football-test-338723!2sjkl_records!3sjkl_standings_yearly) (will only work if you have permissions)

## Requirements to run:
Some files with private info (e.g. credentials) are excluded from the repo. In order to run the DAG
successfully, one needs to do the following:
- configure SMTP settings in airflow.cfg file with credentials of an email account for the 
EmailOperator to send emails from (example: https://naiveskill.com/send-email-from-airflow/). 
Excluded from public repo since it contains a password for the 'from' email address
- include the GCP credentials JSON file in root directory (e.g. 'fantasy-football-test-338723-3503d10af898.json'
is referenced in my scripts here but excluded from the public repo for security)