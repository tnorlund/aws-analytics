import os
import sys
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisitor, itemToVisit, itemToSession # pylint: disable=wrong-import-position
from dynamo.entities import itemToLocation, itemToBrowser # pylint: disable=wrong-import-position

dynamo = boto3.client( 'dynamodb' )

def getVisitorDetails( visitor ):
  '''Gets the visitor and their details from the table.

  Parameters
  ----------
  visitor : Visitor
    The visitor to request from the table.

  Returns
  -------
  result : dict
    The result of requesting the visitor from the table. This contains either
    the error that occurred or the visitor's details.
  '''
  try:
    result = dynamo.query(
      TableName = os.environ.get( 'TABLE_NAME' ),
      KeyConditionExpression = '#pk = :pk',
      ExpressionAttributeNames = { '#pk': 'PK' },
      ExpressionAttributeValues = { ':pk': visitor.pk() },
      ScanIndexForward = True
    )
    if len( result['Items'] ) == 0:
      return { 'error': 'Visitor not in table' }
    data = { 'visits': [], 'browsers': [], 'sessions': [] }
    for item in result['Items']:
      if item['Type']['S'] == 'visitor':
        data['visitor'] = itemToVisitor( item )
      elif item['Type']['S'] == 'visit':
        data['visits'].append( itemToVisit( item ) )
      elif item['Type']['S'] == 'session':
        data['sessions'].append( itemToSession( item ) )
      elif item['Type']['S'] == 'location':
        data['location'] = itemToLocation( item )
      elif item['Type']['S'] == 'browser':
        data['browsers'].append( itemToBrowser( item ) )
      else:
        raise Exception(
          f'''Could not parse type: { item }'''
        )
    return data
  except KeyError as e:
    print( f'ERROR getVisitorDetails: {e}')
    return { 'error': 'Could not get visitor from table' }
  except ClientError as e:
    print( f'ERROR getVisitorDetails: { e }')
    return { 'error': 'Could not get visitor from table' }
