import requests
import pandas as pd
import json
import boto3
import logging
from botocore.exceptions import ClientError

url = "https://twitter154.p.rapidapi.com/user/tweets"
querystring = {"username": "elonmusk", "limit": "100","include_replies":"false","include_pinned":"false"}

with open("Api_keys.json") as api_keys_file:
    api_keys = json.load(api_keys_file)

with open("aws_credentials.json") as aws_credentials:
    aws_keys=json.load(aws_credentials)

bucket = "twitter-api-data-storage"

def get_X_data():
    try:
        response = requests.get(url, headers=api_keys, params=querystring)
        response.raise_for_status() 
        tweets = response.json()
        # print(tweets)
        return tweets.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

data_list = []
def X_df(results, data_list=None):
    if not data_list:  # If data_list is empty
        data_list = []  # Initialize it as an empty list
    
    for result in results:
        user_details = result.get('user', {})
        data_list.append({
            'User Name': user_details.get('username', ''),
            'Creation Date': result.get('creation_date', ''),
            'Text': result.get('text', ''),
            'Likes': result.get('favorite_count', 0),
            'Retweet Count': result.get('retweet_count', 0),
            'Reply Count': result.get('reply_count', 0),
            'Views': result.get('views', 0)
        })
    return pd.DataFrame(data_list)

def df_to_S3(df,bucket,aws_keys):
    s3=boto3.client('s3', aws_access_key_id=aws_keys["aws_access_key_id"],
                    aws_secret_access_key=aws_keys["aws_secret_access_key"])
    try:
        csv_buffer = df.to_csv(index=False)
        response = s3.put_object(Body=csv_buffer, Bucket=bucket, Key='tweets.csv')
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == "__main__":
    results = get_X_data()
    if results:
        df = X_df(results)
        if df_to_S3(df,bucket,aws_keys):
            print("DataFrame uploaded to S3 successfully.")
        else:
            print("Error uploading DataFrame to S3.")


