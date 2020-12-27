import os
import sys
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Location # pylint: disable=wrong-import-position

class _Location:
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
      The result of removing the location from the table.
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
