import json
import os
import sys
import io
import datetime
import numpy as np
import pandas as pd
import pyarrow # pylint: disable=unused-import
import urllib3
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.processing import processDF, processVisits # pylint: disable=wrong-import-position
from dynamo.entities import Visitor # pylint: disable=wrong-import-position
from dynamo.entities import requestToLocation # pylint: disable=wrong-import-position

http = urllib3.PoolManager()

def processParquet( key, dynamo_client, s3_client ):
  '''Adds the data from a '.parquet' file to the DynamoDB table.

  Parameters
  ----------
  key : str
    The key of the '.parquet' file in the S3 bucket.
  dynamo_client : DynamoClient
    The DynamoDB client used to store the transformed data.
  s3_client : S3Client
    The S3 client used to get the '.parquet' file from.
  '''
  try:
    request = s3_client.getObject( key )
    # Read the parquet file as a pandas DF
    df = pd.read_parquet( io.BytesIO( request['Body'].read() ) )
    # Get the unique IP addresses
    ips = df['ip'].unique()
    # Iterate over the IP addresses to organize the DF's per visitor
    for ip in ips:
      # Get the visitor details from the table.
      visitor_details = dynamo_client.getVisitorDetails( Visitor( ip ) )
      # Get the browsers and visits of the specific IP address.
      visitor_dict = processDF( df, ip )
      # When the visitor is not found in the database, the visitor, location,
      # browser, session, and visits must be added to the database.
      if 'error' in visitor_details.keys() \
        and visitor_details['error'] == 'Visitor not in table':
        # Add the new visitor and their data to the table
        _createNewVisitor(
          ip, visitor_dict['browsers'], visitor_dict['visits'], dynamo_client
        )
      # Otherwise, determine whether to add a new session or update the
      # visitor's last session.
      else:
        # Calculate the end datetime of the visitor's last session. This is the
        # session's starting datetime plus the total time of the session.
        lastSession = visitor_details['sessions'][-1]
        lastSessionEnd = lastSession.sessionStart + \
          datetime.timedelta( seconds=lastSession.totalTime )
        # Update the session to include these visits when the time between the
        # end of the last session and the first visit is less than 30 minutes.
        if ( visitor_dict['visits'][0].date - lastSessionEnd ).days < 1 \
          and (
            visitor_dict['visits'][0].date - lastSessionEnd
        ).seconds/3600 < 0.5:
          _updateSession( lastSession, visitor_dict['visits'], dynamo_client )
        # Create a new session when the time between the last session and the
        # first of these visits is greater than 30 minutes.
        else:
          _addSessionToVisitor(
            ip, visitor_dict['visits'], visitor_dict['browsers'], dynamo_client
          )
  except Exception as e:
    print( f'ERROR processParquet { e }' )
    print(
      f'Error getting object { key } from bucket { s3_client.bucketname }.' + \
        ' Make sure they exist and your bucket is in the same region as ' + \
        'this function.'
    )
    raise e

def _createNewVisitor( ip, browsers, visits, dynamo_client ):
  '''Adds new Visitor data from a visitor-specific DataFrame to the table.

  Parameters
  ----------
  ip : str
    The IP address of the visitor.
  v_df : pd.DataFrame
    The visitor-specific DataFrame that holds the session's data.
  dynamo_client : DynamoClient
    The DynamoDB client used to access the table

  Returns
  -------
  result : dict
    The result of adding the new visitor and their data to the table. This
    could be new visitor, location, browser, session, and visits added or the
    error that occurred.
  '''
  result = dynamo_client.addNewVisitor(
    Visitor( ip, 1 ), # Visitor
    requestToLocation( json.loads(
      http.request(
        'GET',
        f'''https://geo.ipify.org/api/v1?apiKey={ os.environ.get('IPIFY_KEY')
        }&ipAddress={ ip }'''
      ).data.decode( 'utf8' )
    ) ), # Location
    browsers, # Browsers
    visits # Visits
  )
  if 'error' in result.keys():
    print( 'ERROR _createNewSession ' + result['error'] )
  return result

def _addSessionToVisitor( ip, visits, browsers, dynamo_client ):
  '''Creates a new Session with the data from a visitor-specific DataFrame.

  Parameters
  ----------
  ip : str
    The IP address of the visitor.
  v_df : pd.DataFrame
    The visitor-specific DataFrame that holds the session's data.
  dynamo_client : DynamoClient
    The DynamoDB client used to access the table.
  visits : list[ Visit ]
    The list of visits found in the parquet file.

  Returns
  -------
  result : dict
    The result of adding the new visitor and their data to the table. This
    could be new visitor, location, browser, session, and visits added or the
    error that occurred.
  '''
  result = dynamo_client.addNewSession(
    Visitor( ip ), # Visitor
    browsers, # Browsers
    visits # Visits
  )
  if 'error' in result.keys():
    print( 'ERROR _addSessionToVisitor ' + result['error'] )
  return result

def _updateSession( lastSession, visits, dynamo_client ):
  '''Updates the latest session with the visits in this '.parquet' file.

  Parameters
  ----------
  lastSession : Session
    The visitor's latest session returned from the database. This is used to
    update with the visits found in this '.parquet' file.
  visits : list[ Visit ]
    The visits found in this '.parquet' file. These are added to the latest
    session and then added to the table.
  dynamo_client : DynamoClient
    The DynamoDB client used to access the table.

  Returns
  -------
  result : dict
    The result of adding the updated session to the table. This could be the
    updated session or the error that occurs while updating the session.
  '''
  result = dynamo_client.getSessionDetails( lastSession )
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
  result = dynamo_client.updateSession( result['session'], visits )
  if 'error' in result.keys():
    print( 'ERROR _updateSession ' + result['error'] )
  return result
