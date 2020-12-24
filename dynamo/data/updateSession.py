import os
import boto3
from botocore.exceptions import ClientError

dynamo = boto3.client(
  'dynamodb', 
  region_name = os.environ.get( 'REGION_NAME' )
)

def updateSession( session, visits ):
  '''Updates a session with new visits and attributes.

  Parameters
  ----------
  session : Session
    The session to change the average time on page and the total time on the
    website.
  visits : list[ Visit ]
    All of the visits that belong to the session.
  '''
  result = addSession( session )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  result = addVisits( visits )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  return { 'session': session, 'visits': visits }

def addSession( session ):
  '''Adds a session to the table.

  Parameters
  ----------
  session : Session
    The visitor's session to be added to the table.

  Returns
  -------
  result : dict
    The result of adding the session to the table.
  '''
  try:
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = session.toItem()
    )
    return { 'session': session }
  except ClientError as e:
    print( f'ERROR addSession: { e }')
    return { 'error': 'Could not add new session to table' }

def addVisits( visits ):
  '''Adds the visits to the table.

  Parameters
  ----------
  visits : list[Visit]
    The visits to be added to the table.

  Returns
  -------
  result : dict
    The result of adding the visits to the table.
  '''
  try:
    result = dynamo.batch_write_item(
      RequestItems = { os.environ.get( 'TABLE_NAME' ): [
        { 'PutRequest': { 'Item': visit.toItem() } }
        for visit in visits
      ] },
    )
    return { 'visits': result }
  except ClientError as e:
    print( f'ERROR addSession: { e }')
    return { 'error': 'Could not add new session to table' }
