import os
import sys
import numpy as np
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Session
dynamo = boto3.client( 'dynamodb' )

def addNewSession( visitor, browsers, visits ):
  '''Adds a new session to the table for the given visitor.

  Parameters
  ----------
  visitor : Visitor
    The returning visitor. They will have their number of sessions incremented.
  browsers : list[ Browser ]
    The visitor's browsers to be added to the table.
  visits: list[ Visit ]
    The visits to be added to the table.

  Returns
  -------
  result : dict
    The result of adding a new session for a visitor. This could be either the
    error that occurs or the updated visitor, the browsers added, and the
    visits added to the table.
  '''
  result = incrementSessions( visitor )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  visitor = result['visitor']
  result = addBrowsers( browsers )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  result = addSession(
    Session(
      visits[0].date,
      visits[0].ip,
      np.mean( [
        visit.timeOnPage for visit in visits
        if type(visit.timeOnPage) == float
      ] ),
      ( visits[-1].date - visits[0].date ).total_seconds()
    )
  )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  result = addVisits( visits )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  return { 'visitor': visitor, 'browsers': browsers, 'visits': visits }

def incrementSessions( visitor ):
  '''Increments the number of sessions a visitor has.

  Parameters
  ----------
  visitor : Visitor
    The visitor to have their number of sessions incremented.

  Returns
  -------
  result : dict
    The result of incrementing the number of sessions the visitor has. This
    could be either an error or the updated visitor.
  '''
  try:
    result = dynamo.update_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Key = visitor.key(),
      ConditionExpression = 'attribute_exists(PK)',
      UpdateExpression= 'SET #count = #count + :inc',
      ExpressionAttributeNames = { '#count': 'NumberSessions' },
      ExpressionAttributeValues= { ':inc': { 'N': '1' } },
      ReturnValues= 'ALL_NEW'
    )
    if 'Attributes' not in result.keys():
      return { 'error': 'Could not find visitor' }
    visitor.numberSessions = int( result['Attributes']['NumberSessions']['N'] )
    return { 'visitor': visitor }
  except ClientError as e:
    print( f'ERROR incrementSessions: { e }' )
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return { 'error': 'Visitor not in table' }
    return { 'error': 'Could not increment the number of sessions of visitor' }

def addBrowsers( browsers ):
  '''Adds the visitor's browser to the table.

  Parameters
  ----------
  browsers : list[ Browser ]
    The visitor's browser to be added to the table.

  Returns
  -------
  result : dict
    The result of adding the browser to the table.
  '''
  try:
    dynamo.batch_write_item(
      RequestItems = { os.environ.get( 'TABLE_NAME' ): [
        { 'PutRequest': { 'Item': browser.toItem() } }
        for browser in browsers
      ] },
    )
    return { 'browsers': browsers }
  except ClientError as e:
    print( f'ERROR addBrowser: { e }')
    return { 'error': 'Could not add new browsers to table' }

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
      Item = session.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    return { 'session': session }
  except ClientError as e:
    print( f'ERROR addSession: { e }')
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return {
        'error': f'Visitor\'s session is already in table { session }'
      }
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