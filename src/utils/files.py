import os
from pathlib import Path
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# dotenv_path = Path('city-bike.env')
# load_dotenv(dotenv_path=dotenv_path)
class AwsStorage():
 
    def __init__(self,region_name:str,aws_access_key_id:str,aws_secret_access_key:str,bucket_name:str):               
        self._service_name='s3'
        self.__aws_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key
        self._aws_region_name = "us-west-2" #region_name,
        self._bucket_name = bucket_name
        
    def __dir__(self):
        return ['list_file_in_s3_folder','create_bucket','bucket_list']
    

    
    
    def create_bucket(self,bucket_name):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if self._aws_region_name is None:
                s3_client = boto3.client('s3', 
                          #region_name=self._aws_region_name, 
                          aws_access_key_id= self.__aws_key_id, 
                          aws_secret_access_key=self.__aws_secret_access_key)
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', 
                          region_name=self._aws_region_name, 
                          aws_access_key_id= self.__aws_key_id, 
                          aws_secret_access_key=self.__aws_secret_access_key)
                location = {'LocationConstraint': region_name}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def bucket_list(self):
        print('Existing buckets:')
        s3_client = boto3.client('s3', 
                          region_name=self._aws_region_name, 
                          aws_access_key_id= self.__aws_key_id, 
                          aws_secret_access_key=self.__aws_secret_access_key)
        bucket_response = s3_client.list_buckets()
        for bucket in bucket_response['Buckets']:
            print(f'  {bucket["Name"]}')

    

    #def list_file_in_s3_folder(bucket_name,folder_name,region_name=AWS_REGION,aws_access_key_id=AWS_KEY_ID,aws_secret_access_key=AWS_SECRET):
    def list_file_in_s3_folder(self,folder_name):
        s3 = boto3.client("s3", 
                          region_name=self._aws_region_name, 
                          aws_access_key_id= self.__aws_key_id, 
                          aws_secret_access_key=self.__aws_secret_access_key)
        response = s3.list_objects_v2(Bucket=self._bucket_name, Prefix=folder_name)
        for object in response['Contents']:
            print(object['Key'])

    def create_folder(self,bucket,directory):
        
        s3 = boto3.client('s3', 
                          region_name=self._aws_region_name, 
                          aws_access_key_id=self.__aws_key_id, 
                          aws_secret_access_key=self.__aws_secret_access_key)
        bucket_name = bucket
        directory_name = directory #it's name of your folders
        s3.put_object(Bucket=bucket_name, Key=(directory_name+'/'))


    def copy_folder_to_s3(self,local_path,bucket_name,s3_folder):
        
        client = boto3.client('s3', 
                          region_name=self._aws_region_name, 
                          aws_access_key_id= self.__aws_key_id, 
                          aws_secret_access_key=self.__aws_secret_access_key)

        local_path = local_path
        bucketname = bucket_name

        for path, dirs, files in os.walk(local_path):
            for file in files:
                s3_path = s3_folder
                file_s3 = os.path.normpath(s3_path + '/' + local_path +  file)
                file_local = os.path.join(path, file)
                print("Upload:", file_local, "to target:", file_s3, end="")
                client.upload_file(file_local, bucketname, file_s3)
                print(" ...Success")



     
    
    

            
