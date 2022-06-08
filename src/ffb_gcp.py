""" Script to extract historical fantasy football standings and upload to GCP Cloud Storage and BigQuery.

For a given ESPN fantasy football league, this script uses the espn-api package to pull historical standings records, 
reorganize and save them into a CSV saved locally, upload that CSV to GCP Cloud Storage, then 
copy data from Cloud Storage to a BigQuery table.

Steps to run: 
- ensure req'd packages are installed in environment:
    - pip install --upgrade espn-api google-cloud-storage google-cloud-bigquery
- set vars for ESPN league (e.g. LEAGUE_ID, ESPN_S2, SWID, LEAGUE_NICKNAME)
- set GCP vars (KEY_PATH is the path to the json file where GCP credentials are stored): 
- add GOOGLE_APPLICATION_CREDENTIALS variable in Airflow, also with value of path to the json file where GCP credentials are stored
"""

__author__ = "Mike Moyer"

import os
import sys
import csv
from espn_api.football import League
from google.cloud import storage, bigquery
from google.oauth2 import service_account

AIRFLOW_PATH = sys.argv[1] # airflow_path passed in via argument in ffb_gcp_dag.py

# ESPN league variables
LEAGUE_ID = 506169
ESPN_S2 = "AEAPiKnJ714Y247Ddp0dE8y%2Bw3RojjYbLozlgOJlPes1VHBVVMew%2BRlVfqFULsWCHewg%2BL7LEuEcjgPjaUesYcdvhxMyQ2xq5JhnisGsVOSPnAr8Zezmesl3aSrKZ71b%2BjQVbNJme8pIe3j62ZvEBTvytK0LoiWHqVhheNgUf4sidQkb9j2YDovHWHPtlGUofjC0RnuH%2F%2F2eFqY8h4MNWyV%2F1SrIdJPGNFf5VkcPjTzeJgZUBJWcrBPtz8ddgzs59LEbshOyjATQwUug1AhIL4D3"
SWID = "{61869D6A-4A59-4596-8587-7ABB8103FD41}"
LEAGUE_NICKNAME = "JKL"

# variables for local file(s)
OUTPUT_FILE_NAME = LEAGUE_NICKNAME + "_historical.csv"
OUTPUT_FILE_PATH = AIRFLOW_PATH + '/output_files/' + OUTPUT_FILE_NAME
COLUMNS = {"year":"INTEGER","owner":"STRING","team_name":"STRING","wins":"INTEGER","losses":"INTEGER","ties":"INTEGER","win_pct":"FLOAT","pts_for":"FLOAT","ppg":"FLOAT","pts_against":"FLOAT","playoff_finish":"INTEGER","reg_season_finish":"INTEGER"}

# GCP variables for storing files
BUCKET_NAME = "fantasy-fb-bucket"
KEY_PATH = AIRFLOW_PATH + "/fantasy-football-test-338723-3503d10af898.json" # path to GCP service account credentials


def create_empty_local_csv(file_path, columns):
    '''Removes any existing local CSV file and recreates empty file with column names as the first row'''
    with open(file_path, 'w') as my_csv:
        csvWriter = csv.writer(my_csv,delimiter=',')
        csvWriter.writerow(columns.keys())

def get_team_results(team, year):
    '''Returns a list with an individual team's standings results for a given year'''

    games = team.wins + team.losses + team.ties
    ppg = team.points_for / games
    win_pct = (team.wins + team.ties/2) / games

    return [year,team.owner,team.team_name,team.wins,team.losses,team.ties,win_pct,team.points_for,ppg,team.points_against,team.final_standing,team.standing]

def get_year_standings(year):
    '''Returns final standings for a given league year in the form of a 2D list'''

    league = League(LEAGUE_ID, year, ESPN_S2, SWID)
    standings = league.standings()
    output = []
    
    for team in standings:
        team_results = get_team_results(team, year)
        # add the individual team's stats for the year
        output.append(team_results)

    return output

def write_year_to_csv(standings, output_file):
    '''Writes an individual year of league standings to a CSV file'''

    with open(output_file,"a") as my_csv:
	    csvWriter = csv.writer(my_csv,delimiter=',')
	    csvWriter.writerows(standings)

def upload_blob_to_gcp(bucket_name, source_file_name, destination_blob_name):
    '''Uploads a file to the bucket'''
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    # The Client() function with no credentials argument automatically looks for credentials in the location defined in $GOOGLE_APPLICATION_CREDENTIALS environment variable 
    # For running on Airflow, use from_service_account_json() to explicitly pass the path to the JSON credentials file
    storage_client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded as {} in bucket {}.".format(
            source_file_name, destination_blob_name, bucket_name
        )
    )

def load_csv_into_bq_table(uri, table_id, schema_dict=None):
    '''Copies data from a CSV in GC Storage into a BigQuery table'''

    # Construct a BigQuery client object.
    credentials = service_account.Credentials.from_service_account_file(
        KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    if schema_dict:
        schema = []
        for key, value in schema_dict.items():
            schema.append(bigquery.SchemaField(key, value))

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV, # source format defaults to CSV, so this line is optional, but included for clarity
            write_disposition="WRITE_TRUNCATE" # replace entire contents if table already exists
        )
    else:
        job_config = bigquery.LoadJobConfig(
            autodetect=True, # use autodetect if no explicit schema is provided
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV, # source format defaults to CSV, so this line is optional, but included for clarity
            write_disposition="WRITE_TRUNCATE" # replace entire contents if table already exists
        )

    # Load data into BigQuery table
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {destination_table.num_rows} rows into table {destination_table.dataset_id}.{destination_table.table_id}.")

def main(start_year, end_year):
    # create local CSV file
    create_empty_local_csv(OUTPUT_FILE_PATH, COLUMNS)

    # write each year in range to local CSV file
    print("Starting...")
    for year in range(start_year, end_year+1):
        print('Loading data for ' + str(year) + '...')
        write_year_to_csv(get_year_standings(year), OUTPUT_FILE_PATH)

    print("Local file complete")

    # can comment out next 7 lines to reduce GCP usage. Leave uncommented to include push to GCP
    # upload CSV file to GCP
    upload_blob_to_gcp(BUCKET_NAME, OUTPUT_FILE_PATH, OUTPUT_FILE_NAME)

    # load data from GCP CSV file into BigQuery table
    uri = f"gs://{BUCKET_NAME}/{OUTPUT_FILE_NAME}"
    table_id = "fantasy-football-test-338723.jkl_records.jkl_standings_yearly"
    load_csv_into_bq_table(uri, table_id, COLUMNS)

if __name__ == "__main__":
    # We switched to a new scoring system (not tracked in ESPN league) in 2019, so only pulling historical records from start of league (2008) up to 2018
    main(2008, 2018)
