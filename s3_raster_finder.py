# %%
import boto3
import re,os
import pandas as pd

with open(r"Z:\LWI\LWI S3 Key.txt") as secret_file:
    secret = secret_file.readlines()
secret_ID = secret[1].strip('\n')
secret_key = secret[3].strip('\n')

# %%
client = boto3.client('s3',
                   aws_access_key_id=secret_ID,
                   aws_secret_access_key = secret_key)
session = boto3.Session(aws_access_key_id=secret_ID,
                        aws_secret_access_key = secret_key)
s3 = session.resource('s3')
response = client.list_buckets()
bucket_names = [bucket['Name'] for bucket in response['Buckets']]
# Limiting to buckets wanted.
bucket_names = [
 'lwi-region7',
 ]

# %%
rasters = {}
for bucket in bucket_names:
    print('\nSearching Bucket:', bucket)
    my_bucket = s3.Bucket(bucket)
    rasters[bucket] = {
        'rasters': []
    }

    for obj in my_bucket.objects.all():
        wanted_keys = ['tif','tiff','geotif', 'geotiff']
        unwanted_keys = ['HMS', 'WSE', 'Depth', 'Manning', 'manning', 'NLCD', 'LandCover', 
                         'LULC', 'LandUse', 'Land_use', 'FIA', 'hydrology', 'demsd8', 'flowaccum', 
                         'flowdir', 'reconditioned', 'sinkfill', 'sinklocs', 'str_bin']
        # extend unwanted_keys with upper and lower case versions, and first letter capitalized
        unwanted_keys.extend([key.upper() for key in unwanted_keys])
        unwanted_keys.extend([key.lower() for key in unwanted_keys])
        unwanted_keys.extend([key.capitalize() for key in unwanted_keys])
        if not any([re.search(key, obj.key) for key in unwanted_keys]):
            if obj.key.endswith(tuple(wanted_keys)):
                rasters[bucket]['rasters'].append({
                    'filename': obj.key,
                    'filesize (GB)': round((obj.size / (1024**3)),4),  # Convert to GB
                    'last_modified': obj.last_modified.strftime('%d%b%Y')
                })
    rasters[bucket]['raster_cnt'] = len(rasters[bucket]['rasters'])
print('Done.')

# %%
# save rasters to a json file
import json
with open('rasters.json', 'w') as f:
    json.dump(rasters, f)
# rasters

# %%
print(unwanted_keys)
# %%
