from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from pymongo import MongoClient
import pandas as pd
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'etl_pipeline_full',
    default_args=default_args,
    description='ETL Pipeline from MongoDB to S3 with transformations',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data from MongoDB Atlas
    def extract_from_mongodb():
        logging.info("Connecting to MongoDB Atlas")
        client = MongoClient("mongodb+srv://<username>:<password>@<database>.eg0ck.mongodb.net/<collection>")
        db = client["Gradify_temp_data"]
        collection = db["temp"]

        logging.info("Fetching data from MongoDB collection")
        data = list(collection.find({}))
        df = pd.DataFrame(data)
        if "_id" in df.columns:
            df.drop(columns=["_id"], inplace=True)

        local_path = '/path/to/log_data.csv'
        df.to_csv(local_path, index=False)
        logging.info(f"Data extracted and saved to {local_path}")

    extract_task = PythonOperator(
        task_id='extract_from_mongodb',
        python_callable=extract_from_mongodb,
    )

    # Task 2: Transform data (split logs into separate CSVs)
    def transform_data():
        logging.info("Loading data for transformation")
        log_data_path = '/tmp/log_data.csv'
        log_data = pd.read_csv(log_data_path)

        logging.info("Transforming data into multiple tables")

        # Create Artists Table
        artists_df = log_data[['artist', 'location']].dropna().drop_duplicates().reset_index(drop=True)
        artists_df['artist_id'] = (artists_df.index + 1).astype('int64')
        artists_df = artists_df[['artist_id', 'artist', 'location']]
        artists_df.rename(columns={'artist': 'name'}, inplace=True)
        artists_df.to_csv('/tmp/artists_table.csv', index=False)
        logging.info("Artists table saved as /tmp/artists_table.csv")

        # Create Songs Table
        songs_df = log_data[['song', 'artist', 'length']].dropna().drop_duplicates().reset_index(drop=True)
        songs_df = songs_df.merge(artists_df[['artist_id', 'name']], left_on='artist', right_on='name', how='left')#what is how
        songs_df = songs_df[songs_df['artist_id'].notna()]
        songs_df['artist_id'] = songs_df['artist_id'].astype('int64')
        songs_df['song_id'] = (songs_df.index + 1).astype('int64')
        songs_df = songs_df[['song_id', 'song', 'artist_id', 'length']]
        songs_df.rename(columns={'song': 'title', 'length': 'duration'}, inplace=True)
        songs_df.to_csv('/tmp/songs_table.csv', index=False)
        logging.info("Songs table saved as /tmp/songs_table.csv")

        # Create Users Table
        users_df = log_data[['userId', 'firstName', 'lastName', 'gender', 'level']].dropna().drop_duplicates().reset_index(drop=True)
        users_df['user_id'] = users_df['userId'].astype('int64')
        users_df = users_df[['user_id', 'firstName', 'lastName', 'gender', 'level']]
        users_df.rename(columns={'firstName': 'first_name', 'lastName': 'last_name'}, inplace=True)
        users_df.to_csv('/tmp/users_table.csv', index=False)
        logging.info("Users table saved as /tmp/users_table.csv")

        # Create Time Table
        log_data['ts'] = pd.to_datetime(log_data['ts'], unit='ms')
        time_df = pd.DataFrame()
        time_df['start_time'] = log_data['ts']
        time_df['hour'] = log_data['ts'].dt.hour
        time_df['day'] = log_data['ts'].dt.day
        time_df['week'] = log_data['ts'].dt.isocalendar().week
        time_df['month'] = log_data['ts'].dt.month
        time_df['year'] = log_data['ts'].dt.year
        time_df['weekday'] = log_data['ts'].dt.weekday
        time_df.drop_duplicates(inplace=True)
        time_df.to_csv('/tmp/time_table.csv', index=False)
        logging.info("Time table saved as /tmp/time_table.csv")

        # Create SongPlays Table
        songplays_df = log_data[log_data['page'] == 'NextSong']
        songplays_df = songplays_df.merge(songs_df[['song_id', 'title']], left_on='song', right_on='title', how='left')
        songplays_df = songplays_df.merge(artists_df[['artist_id', 'name']], left_on='artist', right_on='name', how='left')
        songplays_df = songplays_df.merge(users_df[['user_id', 'first_name']], left_on='userId', right_on='user_id', how='left')
        songplays_df = songplays_df.merge(time_df[['start_time']], left_on='ts', right_on='start_time', how='left')
        songplays_df['songplay_id'] = (songplays_df.index + 1).astype('int64')
        songplays_df = songplays_df[['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']]
        songplays_df.rename(columns={'sessionId': 'session_id', 'userAgent': 'user_agent'}, inplace=True)
        songplays_df.to_csv('/tmp/songplays_table.csv', index=False)
        logging.info("SongPlays table saved as /tmp/songplays_table.csv")

        logging.info("Data transformation completed")

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task 3: Upload transformed CSVs to S3
    def upload_to_s3():
        logging.info("Uploading transformed files to S3")

        aws_conn = BaseHook.get_connection('aws_default')  # Replace with your Airflow connection ID
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password
        )

        files_to_upload = {
            '/tmp/artists_table.csv': 'artists_table.csv',
            '/tmp/songs_table.csv': 'songs_table.csv',
            '/tmp/users_table.csv': 'users_table.csv',
            '/tmp/time_table.csv': 'time_table.csv',
            '/tmp/songplays_table.csv': 'songplays_table.csv',
        }
        bucket_name = "gradify-processed-data"  # Replace with your S3 bucket name

        for local_path, s3_key in files_to_upload.items():
            try:
                s3.upload_file(local_path, bucket_name, s3_key)
                logging.info(f"Successfully uploaded {local_path} to S3 bucket {bucket_name} as {s3_key}")
            except Exception as e:
                logging.error(f"Failed to upload {local_path} to S3: {str(e)}")

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    # Set up task dependencies
    extract_task >> transform_task >> upload_task
    