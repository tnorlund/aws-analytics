import os, sys
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
import numpy as np
from dynamo.entities import Session 
import boto3
from botocore.exceptions import ClientError
dynamo = boto3.client( 'dynamodb' )

def addNewVisitor( visitor, location, browser, visits ):
  '''Adds a new visitor and their details the the table.

  Parameters
  ----------
  visitor : Visitor
    The visitor to be added to the table.
  location : Location
    The visitor's location to be added to the table.
  browser : Browser
    The visitor's browser to be added to the table.
  visits : list[ Visit ]
    The visits to be added to the table.
  
  Returns
  -------
  result : dict
    The result of adding the visitor and their attributes to the table.
  '''
  result = addVisitor( visitor )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  result = addLocation( location )
  if 'error' in result.keys():
    return { 'error': result['error'] }
  result = addBrowser( browser )
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
  return {
    'visitor': visitor, 'location': location, 'browser': browser,
    'visits': visits
  }
  

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


def addBrowser( browser ):
  '''Adds the visitor's browser to the table.

  Parameters
  ----------
  browser : Browser
    The visitor's browser to be added to the table.

  Returns
  -------
  result : dict
    The result of adding the browser to the table.
  '''
  try:
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = browser.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    return { 'browser': browser }
  except ClientError as e:
    print( f'ERROR addBrowser: { e }')
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return { 
        'error': f'Visitor\'s browser is already in table { browser }' 
      }
    return { 'error': 'Could not add new location to table' }

def addLocation( location ):
  '''Adds the visitor's location to the table.

  Parameters
  ----------
  location : Location
    The visitor's location to be added to the table.

  Returns
  -------
  result : dict
    The result of adding the visitor's location to the table.
  '''
  try:
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = location.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    return { 'location': location }
  except ClientError as e: 
    print( f'ERROR addLocation: { e }')
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return { 
        'error': f'Visitor\'s location is already in table { location }' 
      }
    return { 'error': 'Could not add new location to table' }

def addVisitor( visitor ):
  '''Adds a visitor to the table.

  Parameters
  ----------
  visitor : Visitor
    The visitor to be added to the table.

  Returns
  -------
  result : dict
    The result of adding the the visitor to the table.
  '''
  try:
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = visitor.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    return { 'visitor': visitor }
  except ClientError as e: 
    print( f'ERROR addNewVisitor: { e }' )
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return { 'error': f'Visitor already in table { visitor }' }
    return { 'error': 'Could not add new visitor to table' }