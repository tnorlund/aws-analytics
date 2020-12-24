import json
import os
import sys
import io
import datetime
import boto3
import numpy as np
import pandas as pd
# import pyarrow # pylint: disable=unused-import
import urllib3
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.data import getVisitorDetails, getSession, addNewVisitor # pylint: disable=wrong-import-position
from dynamo.data import addNewSession, updateSession # pylint: disable=wrong-import-position
from dynamo.processing import processDF, processVisits # pylint: disable=wrong-import-position
from dynamo.entities import Visitor, Visit # pylint: disable=wrong-import-position
from dynamo.entities import requestToLocation # pylint: disable=wrong-import-position
from dynamo.entities import Browser # pylint: disable=wrong-import-position

http = urllib3.PoolManager()
s3 = boto3.client(
  's3',
  region_name = os.environ.get( 'REGION_NAME' )
)
dynamo = boto3.client(
  'dynamodb',
  region_name = os.environ.get( 'REGION_NAME' )
)

def processParquet( key ):
  '''Adds the data from a '.parquet' file to the DynamoDB table.

  Parameters
  ----------
  key : str
    The key of the '.parquet' file in the S3 bucket.
  '''
  try:
    request = s3.get_object(
      Bucket = os.environ.get('BUCKET_NAME'),
      Key = key
    )
    # Read the parquet file as a pandas DF
    df = pd.read_parquet( io.BytesIO( request['Body'].read() ) )
    # Get the unique IP addresses
    ips = df['ip'].unique()
    # Iterate over the IP addresses to organize the DF's per visitor
    for ip in ips:
      # Get the visitor details from the table.
      results = getVisitorDetails( Visitor( ip ) )
      # Process the entire dataframe to get the cleaned set
      v_df = processDF( df, ip )
      # When the visitor is not found in the database, the visitor, location,
      # browser, session, and visits must be added to the database.
      if 'error' in results.keys() \
        and results['error'] == 'Visitor not in table':
        # Add the new visitor and their data to the table
        result = addNewVisitor(
          Visitor( ip, 1 ),
          requestToLocation(
            json.loads(
              http.request(
                'GET',
                f'''https://geo.ipify.org/api/v1?apiKey={
                  os.environ.get('IPIFY_KEY')
                }&ipAddress={ ip }'''
              ).data.decode( 'utf8' )
            )
          ),
          [
            Browser(
              row['app'], row['ip'], row['width'], row['height'], row['id']
            )
            for index, row in v_df.loc[
              v_df[
                ['app', 'width', 'height']
              ].drop_duplicates().dropna().index
            ].iterrows()
          ],
          [
            Visit(
              row['id'], row['ip'], row['user'], row['title'], row['slug'],
              v_df.iloc[0]['id'], row['seconds'], row['prevTitle'],
              row['prevSlug'], row['nextTitle'], row['nextSlug']
            ) for index, row in v_df.iterrows()
          ]
        )
      # Otherwise, determine whether to add a new session or update the
      # visitor's last session.
      else:
        # Parse the visits from the visitor's DF
        visits = [
          Visit(
            row['id'], row['ip'], row['user'], row['title'], row['slug'],
            v_df.iloc[0]['id'], row['seconds'], row['prevTitle'],
            row['prevSlug'], row['nextTitle'], row['nextSlug']
          ) for index, row in v_df.iterrows()
        ]
        # Calculate the end datetime of the last session
        lastSession = results['sessions'][-1]
        lastSessionEnd = lastSession.sessionStart + \
          datetime.timedelta( seconds=lastSession.totalTime )
        # When the time since the last session is less than 30 min ago, update
        # the session to include these visits.
        if (visits[1].date - lastSessionEnd).days < 1 \
          and (visits[1].date - lastSessionEnd).seconds/3600 < 0.5:
          result = _handleSessionUpdate( lastSession, visits )
        # Otherwise, add the browser, create a new session, and add the visits.
        else:
          result = addNewSession(
            Visitor( ip ),
            [
              Browser(
                row['app'], row['ip'], row['width'], row['height'], row['id']
              )
              for index, row in v_df.loc[
                v_df[
                  ['app', 'width', 'height']
                ].drop_duplicates().dropna().index
              ].iterrows()
            ],
            visits
          )
          if 'error' in result.keys():
            print( 'error' )
            print( result['error'] )
  except Exception as e:
    print( f'ERROR processParquet { e }' )
    print(
      f'''Error getting object {
        key
      } from bucket {
        os.environ.get( 'BUCKET_NAME' )
      }. Make sure they exist and your bucket is in the same region as this''' \
      + 'function.'
    )
    raise e


def _handleSessionUpdate( lastSession, visits ):
  '''Updates that latest session with the visits in this '.parquet' file.

  Parameters
  ----------
  lastSession : Session
    The visitor's latest session returned from the database. This is used to
    update with the visits found in this '.parquet' file.
  visits : list[ Visit ]
    The visits found in this '.parquet' file. These are added to the latest
    session and then added to the table.

  Returns
  -------
  result : dict
    The result of adding the updated session to the table. This could be the
    updated session or the error that occurs while updating the session.
  '''
  result = getSession( lastSession )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  # Combine the previous visits and the ones in this S3 PUT
  visits = processVisits( visits + result['visits'] )
  # Update the previous session to have the attributes with the new
  # visits.
  result['session'].avgTime = np.mean( [
    visit.timeOnPage for visit in visits
    if isinstance( visit.timeOnPage, float )
  ] )
  result['session'].totalTime = (
    visits[-1].date - visits[0].date
  ).total_seconds()
  # Add the updated session and visits to the table.
  result = updateSession( result['session'], visits )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  return { 'session': result['Session'], 'visits': result['visits'] }
