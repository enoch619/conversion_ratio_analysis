#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 17 23:25:16 2019

@author: echeun06
"""


import pandas as pd
from google.cloud import storage
from google.cloud.storage import Blob

def list_blobs(bucket):
    print("Retrieving blobs from GCS")
    blobs = bucket.list_blobs()
    blob_list = [blob for blob in blobs]
    return blob_list

def download_blob(blob):
    print("Downloading {name} from GCS".format(name=blob.name))
    blob.download_to_filename('./tmp/' + blob.name)

def upload_to_gcs(blob, filepath):
    blob.upload_from_filename(filepath)

def key(f1, f2):
    df1 = pd.read_csv('./tmp/{filename}'.format(filename=f1), delimiter=',')
    df1 = df1.loc[:, :"Media Cost (Advertiser Currency)"] 
    df1['key'] = df1['Advertiser'] + '_' + df1['Date']
    df2 = pd.read_csv('./tmp/{filename}'.format(filename=f2), delimiter=',')
    df2 = df2.loc[:, :"Floodlight Impressions"]
    df2['key'] = df2['Advertiser'] + '_' + df2['Date']
    df2 = df2[['key','Floodlight Impressions']]
    df2['Floodlight Total'] = df2.groupby(['key'])['Floodlight Impressions'].transform('sum')
    df2 = df2.drop_duplicates(subset=['key'])
    df = df1.merge(df2, on='key', how='outer')
    return df

def main():
    # TEMPORARILY CONFIGURATION
    FILENAME1 = 'all_conversion_20190117.csv'
    FILENAME2 = 'all_conversion_floodlight_20190117.csv'

    # Authentication
    storage_client = storage.Client.from_service_account_json(
        './amnet-dcm01.json'
    )
    bucket_name = 'amnet_dbm_reports'
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list_blobs(bucket)
    for blob in blobs:
        if FILENAME1 == blob.name:
            download_blob(blob)
        if FILENAME2 == blob.name:
            download_blob(blob)
            
            
    # Transform function
    df = key(FILENAME1, FILENAME2)
    filename_out = FILENAME1.split('_')[1] + '_joined.csv'
    df.to_csv('./{filename}'.format(filename=filename_out), sep=",")
    bucket = storage_client.get_bucket("amnet_dbm_reports")
    blob = Blob(filename_out, bucket)
    upload_to_gcs(blob, './' + filename_out)
    # Upload new table to GCS
    

if __name__ == "__main__":
    main()