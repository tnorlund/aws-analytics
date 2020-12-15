import os, sys
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisit, itemToSession
import boto3
from botocore.exceptions import ClientError
dynamo = boto3.client( 'dynamodb' )

def getSession( session ):
  '''Gets the session and visits from the table.

  Parameters
  ----------
  session : Session
    The session requested from the table.

  Returns
  -------
  data : dict
    The result of getting the session from the table. This contains either the
    error that occurred or the session and its visits.
  '''
  try:
    result = dynamo.query(
      TableName = os.environ.get( 'TABLE_NAME' ),
      IndexName = 'GSI2',
      KeyConditionExpression = '#gsi2 = :gsi2',
      ExpressionAttributeNames = { '#gsi2': 'GSI2PK' },
      ExpressionAttributeValues = { ':gsi2': session.gsi2pk() },
      ScanIndexForward = True
    )
    if len( result['Items'] ) == 0:
      return { 'error': 'Session not in table' }
    data = { 'visits': [] }
    for item in result['Items']:
      if item['Type']['S'] == 'visit':
        data['visits'].append( itemToVisit( item ) )
      elif item['Type']['S'] == 'session':
        data['session'] = itemToSession( item )
      else:
        raise Exception( f'Could not parse type: { item }' )
    return data
  except ClientError as e:
    print( f'ERROR getSession: { e }')
    return { 'error': 'Could not get session from table' }