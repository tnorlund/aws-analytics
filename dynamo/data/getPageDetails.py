import os
import sys
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisit, itemToSession # pylint: disable=wrong-import-position

dynamo = boto3.client( 'dynamodb' )

def getPageDetails( page ):
  '''Gets a page and its days, weeks, months, and years of analytics.

  Parameters
  ----------
  page : Page
    The page to request the details of.

  Returns
  -------
  result : dict
    The result of requesting the page from the table. This contains either the
    error that occurred or the page's analytics.
  '''
  try:
    result = dynamo.query(
      TableName = os.environ.get( 'TABLE_NAME' ),
      IndexName = 'GSI1',
      KeyConditionExpression = '#gsi1 = :gsi1',
      ExpressionAttributeNames = { '#gsi1': 'GSI1PK' },
      ExpressionAttributeValues = { ':gsi1': page.gsi1pk() },
      ScanIndexForward = True
    )
    if len( result['Items'] ) == 0:
      return { 'error': 'Page not in table' }
    data = {
      'visits': []
    }
    for item in result['Items']:
      if item['Type']['S'] == 'visit':
        data['visits'].append( itemToVisit( item ) )
      elif item['Type']['S'] == 'session':
        data['sessions'].append( itemToSession( item ) )
      else:
        raise Exception(
          f'Could not parse type { item }'
        )
    return data
  except KeyError as e:
    print( f'ERROR getPageDetails: {e}')
    return { 'error': 'Could not get page from table' }
  except ClientError as e:
    print( f'ERROR getPageDetails: { e }')
    return { 'error': 'Could not get page from table' }
