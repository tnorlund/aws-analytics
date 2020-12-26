import os
import sys
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Visit, Visitor, Location # pylint: disable=wrong-import-position
from dynamo.entities import itemToVisit, itemToSession # pylint: disable=wrong-import-position

class DynamoClient:
  def __init__( self, tableName, regionName='us-east-1' ):
    '''Constructs the necessary attributes for the DynamoDB client object.

    Parameters
    ----------
    tableName : str
      The name of the DynamoDB table.
    regionName : str
      The AWS region to connect to.
    '''
    self.client = boto3.client( 'dynamodb', region_name = regionName )
    self.tableName = tableName

  def addVisitor( self, visitor ):
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
    if not isinstance( visitor, Visitor ):
      raise ValueError( 'Must pass a Visitor object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = visitor.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'visitor': visitor }
    except ClientError as e:
      print( f'ERROR addVisitor: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Visitor already in table { visitor }' }
      return { 'error': 'Could not add new visitor to table' }

  def removeVisitor( self, visitor ):
    '''Removes a visitor from the table.

    Parameters
    ----------
    visitor : Visitor
      The visitor to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the the visitor to the table.
    '''
    if not isinstance( visitor, Visitor ):
      raise ValueError( 'Must pass a Visitor object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = visitor.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'visitor': visitor }
    except ClientError as e:
      print( f'ERROR removeVisitor: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Visitor not in table { visitor }' }
      return { 'error': 'Could not remove visitor from table' }

  def incrementVisitorSessions( self, visitor ):
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
    if not isinstance( visitor, Visitor ):
      raise ValueError( 'Must pass a Visitor object' )
    try:
      result = self.client.update_item(
        TableName = self.tableName,
        Key = visitor.key(),
        ConditionExpression = 'attribute_exists(PK)',
        UpdateExpression= 'SET #count = #count + :inc',
        ExpressionAttributeNames = { '#count': 'NumberSessions' },
        ExpressionAttributeValues= { ':inc': { 'N': '1' } },
        ReturnValues= 'ALL_NEW'
      )
      visitor.numberSessions = int(
        result['Attributes']['NumberSessions']['N']
      )
      return { 'visitor': visitor }
    except ClientError as e:
      print( f'ERROR incrementSessions: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': 'Visitor not in table' }
      return {
        'error': 'Could not increment the number of sessions of visitor'
      }

  def addLocation( self, location ):
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
    if not isinstance( location, Location ):
      raise ValueError( 'Must pass a Location object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
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

  def removeLocation( self, location ):
    '''Removes a location from the table.

    Parameters
    ----------
    location : Location
      The location to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the the location to the table.
    '''
    if not isinstance( location, Location ):
      raise ValueError( 'Must pass a Location object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = location.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'location': location }
    except ClientError as e:
      print( f'ERROR removeLocation: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Location not in table { location }' }
      return { 'error': 'Could not remove location from table' }

  def addVisit( self, visit ):
    '''Adds a visitor's page visit to the table.

    Parameters
    ----------
    visit : Visit
      The visitor's page visit to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a visitor's page visit to the table.
    '''
    if not isinstance( visit, Visit ):
      raise ValueError( 'Must pass a Visit object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = visit.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'visit': visit }
    except ClientError as e:
      print( f'ERROR addVisit: { e }')
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return {
          'error': f'Visitor\'s page visit is already in table { visit }'
        }
      return { 'error': 'Could not add new page visit to table' }

  def removeVisit( self, visit ):
    '''Removes a visit from the table.

    Parameters
    ----------
    visit : Visit
      The visit to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the the visit to the table.
    '''
    if not isinstance( visit, Visit ):
      raise ValueError( 'Must pass a Visit object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = visit.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'visit': visit }
    except ClientError as e:
      print( f'ERROR removeVisit: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Visit not in table { visit }' }
      return { 'error': 'Could not remove visit from table' }

  def addVisits( self, visits ):
    '''Adds a visitor's page visit to the table.

    Parameters
    ----------
    visits : list[ Visit ]
      The visitor's page visits to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a visitor's page visits to the table.
    '''
    if any( not isinstance( visit, Visit ) for visit in visits ):
      raise ValueError( 'Must pass a Visit objects' )
    try:
      self.client.batch_write_item(
        RequestItems = {
          self.tableName: [
            { 'PutRequest': { 'Item': visit.toItem() } }
            for visit in visits
          ] },
      )
      return { 'visits': visits }
    except ClientError as e:
      print( f'ERROR addVisits: { e }')
      return { 'error': 'Could not add new page visits to table' }

  def getSessionDetails( self, session ):
    '''Gets the session and visits from the table.

    Parameters
    ----------
    session : Session
      The session requested from the table.

    Returns
    -------
    data : dict
      The result of getting the session from the table. This contains either
      the error that occurred or the session and its visits.
    '''
    try:
      result = self.client.query(
        TableName = self.tableName,
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
