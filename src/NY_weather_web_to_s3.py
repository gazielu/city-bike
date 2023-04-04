import os
from utils.wwo_hist_spark import retrieve_city_data,retrieve_hist_data,get_month_dates
from utils.files import AwsStorage
import boto3
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from prefect import flow, task
from prefect_aws.s3 import S3Bucket
import pandas as pd
from prefect_aws import AwsCredentials
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from random import randint



@task(retries=3)
def initial_env(env_file:str) -> None:
    global AWS_KEY_ID, AWS_SECRET, AWS_REGION, STORAGE_BUCKET_NAME, API_KEY
    dotenv_path = Path('utils',env_file)
    load_dotenv(dotenv_path=dotenv_path)
    AWS_KEY_ID = os.getenv('AWS_KEY_ID')
    AWS_SECRET = os.getenv('AWS_SECRET')
    AWS_REGION = os.getenv('AWS_REGION')
    STORAGE_BUCKET_NAME = os.getenv('STORAGE_BUCKET_NAME')
    API_KEY = os.getenv('API_KEY')
    #print(API_KEY,STORAGE_BUCKET_NAME)

@task(retries=3)
def extarct_weather_data(year: str,month: str, location: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    frequency=1
    
    start_date,end_date = get_month_dates(year, month)#'01-DEC-2018'
    #end_date = get_month_dates(2018, 12)#'30-DEC-2018'
    api_key = API_KEY#'25c4af770ad84987be7181434232203'#API_KEY
    location_list = 'new-york'
    #print(frequency,end_date)

    hist_weather_data = retrieve_city_data(api_key,
                                    location_list,
                                    start_date,
                                    end_date,
                                    frequency,
                                    location_label = False,
                                    export_csv = False,
                                    store_df = True)

    
    return hist_weather_data


# @task(log_prints=True)
# def clean(df: pd.DataFrame) -> pd.DataFrame:
#     """Fix dtype issues"""
    
#     df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
#     df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
#     # print(df.head(2))
#     # print(f"columns: {df.dtypes}")
#     print(f"log print rows: {len(df)}")
#     return df


# @task()
# def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
#     """Write DataFrame out locally as parquet file"""
#     path = PurePosixPath("week_2_workflow_orchestration","data",color,f"{dataset_file}.parquet")
#     #path = os.path.join("data",color,f"{dataset_file}.parquet")
#     df.to_parquet(path, compression="gzip")
#     return path



# @task()
# def create_local_folders(color: str) -> None:
#     """ Create data folder for color if not existing"""
#     outdir = os.path.join('week_2_workflow_orchestration','data', color)
#     if not os.path.exists(outdir):
#         os.mkdir(outdir)


# @task()
# def write_gcs(path: Path) -> None:
#     """Upload local parquet file to GCS"""
#     #gcs_block = GcsBucket.load("zoom-gcs")
#     gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-prefect-gcs")
#     gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
#     return

@flow()
def weather_web_to_s3() -> None:
    """The main ETL function"""

    env_file_name = 'city-bike.env'
    location = 'new-york'
    year = 2018
    month = 4
    initial_env(env_file_name)
    print(AWS_REGION)

    df = extarct_weather_data(year,month,location)
    df.to_csv('./tests.csv')
    df.head()
    # df = fetch(dataset_url)


    # df_clean = clean(df)
    # #create_local_folders(color)
    # path = write_local(df_clean, color, dataset_file)
    # # path = path.replace(r'\',r'//')
    # # print(path)
    # #path = r"data/green/green_tripdata_2020-01.parquet"
    # write_gcs(path)


if __name__ == "__main__":
    weather_web_to_s3()

