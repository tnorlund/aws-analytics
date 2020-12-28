import os
import pytest
import pandas as pd
from dynamo.data import S3Client
# , DynamoClient
# from dynamo.processing import processParquet

@pytest.mark.usefixtures(
  'dynamo_client', 'table_init', 's3_client', 's3_init'
)
def test_processParquet(
  # table_name,
  bucket_name, visits, browsers
):
  pd.DataFrame( {
    'id': [
        visit.date.strftime( '%Y-%m-%dT%H:%M:%S.' ) + \
        visit.date.strftime('%f')[:3] + 'Z'
        for visit in visits
    ],
    'title': [visit.title for visit in visits],
    'slug': [visit.slug for visit in visits],
    'ip': [visit.ip for visit in visits],
    'user': [visit.user for visit in visits],
    'app': [browsers[0].app for index in range( len( visits ) )],
    'height': [browsers[0].height for index in range( len( visits ) )],
    'width': [browsers[0].width for index in range( len( visits ) )]
  } ).to_parquet('test.parquet')
  # dynamo_client = DynamoClient( table_name )
  s3_client = S3Client( bucket_name )
  s3_client.putObject( 'test.parquet', 'test.parquet' )
  # processParquet( 'test.parquet', dynamo_client, s3_client )
  os.remove( 'test.parquet' )
