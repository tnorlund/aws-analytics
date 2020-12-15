import os, sys
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisitor, itemToVisit, itemToSession, \
  itemToLocation, itemToBrowser
import boto3
dynamo = boto3.client( 'dynamodb' )

def getVisitorDetails( visitor ):
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
    data = {
      'visits': [],
      'browsers': [],
      'sessions': []
    }
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
  except Exception as e:
    print( f'ERROR getVisitorDetails: {e}')
    return { 'error': 'Could not get visitor from table' }