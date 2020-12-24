import os
import sys
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisit, itemToSession, itemToPage # pylint: disable=wrong-import-position
from dynamo.entities import itemToDay, itemToWeek, itemToMonth, itemToYear # pylint: disable=wrong-import-position

dynamo = boto3.client(
  'dynamodb',
  region_name = os.environ.get( 'REGION_NAME' )
)

def getPageDetails( page ):
  '''Gets a page and its days, weeks, months, and years of analytics.

  Parameters
  ----------
  page : Page
    The page to request the details of.

  Raises
  ------
  Exception
    When the items returned by the query are not either a visit, session, page,
    day, week, month, or year.
  KeyError
    When the items returned are not structured as expected
  ClientError
    When the DynamoDB client raises an exception.

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
    # Return the error when there are no items returned from the table.
    if len( result['Items'] ) == 0:
      return { 'error': 'Page not in table' }
    # Use a dictionary to store the items returned from the table
    data = {
      'visits': [], 'sessions': [], 'days': [], 'weeks': [], 'months': [],
      'years': []
    }
    for item in result['Items']:
      if item['Type']['S'] == 'visit':
        data['visits'].append( itemToVisit( item ) )
      elif item['Type']['S'] == 'session':
        data['sessions'].append( itemToSession( item ) )
      elif item['Type']['S'] == 'page':
        data['page'] = itemToPage( item )
      elif item['Type']['S'] == 'day':
        data['days'].append( itemToDay( item ) )
      elif item['Type']['S'] == 'week':
        data['weeks'].append( itemToWeek( item ) )
      elif item['Type']['S'] == 'month':
        data['months'].append( itemToMonth( item ) )
      elif item['Type']['S'] == 'year':
        data['years'].append( itemToYear( item ) )
      else:
        raise Exception( f'Could not parse type { item }' )
    return data
  except KeyError as e:
    print( f'ERROR getPageDetails: {e}')
    return { 'error': 'Could not get page from table' }
  except ClientError as e:
    print( f'ERROR getPageDetails: { e }')
    return { 'error': 'Could not get page from table' }
