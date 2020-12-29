import re

def formatDate( date ):
  '''Formats a datetime object to a string like JS's ISO standard.

  Parameters
  ----------
  date: datetime.datetime
    The datetime requested to format

  Returns
  -------
  formattedDate : str
    The formatted datetime string that is similar to JS's ISO standard.
  '''
  return date.strftime( '%Y-%m-%dT%H:%M:%S.' ) \
    + date.strftime('%f')[:3] + 'Z'

def objectToItemAtr( obj ):
  '''Formats any Python type to its respective DynamoDB syntax.

  Parameters
  ----------
  obj : any
    The object to be formatted into the DynamoDB syntax.

  Returns
  -------
  result : dict
    The DynamoDB syntax of the object.
  '''
  atr = _objectToItemAtr_singleton( obj )
  if atr is not None:
    return atr
  if isinstance( obj, dict ):
    return { 'M': {
      key: objectToItemAtr( value )
      for key, value in obj.items()
    } }
  atr = _objectToItemAtr_list( obj )
  if atr is not None:
    return atr
  if obj is None:
    return { 'NULL': True }
  raise Exception( 'Could not parse attribute: ', obj )

def _objectToItemAtr_singleton( obj ):
  '''Formats strings, numbers, and booleans into DynamoDB syntax.'''
  if isinstance( obj, str ):
    if obj == 'None':
      return { 'NULL': True }
    return { 'S': obj }
  if isinstance( obj, ( float, int ) ):
    return { 'N': str( obj ) }
  if isinstance( obj, bool ):
    return { 'BOOL': obj }
  return None

def _objectToItemAtr_list( obj ):
  '''Formats lists into DynamoDB syntax.'''
  if isinstance( obj, list ) and \
    all( [re.match( r'[\d\.]+', num ) for num in obj] ):
    return { 'NS': [ str( num ) for num in obj ] }
  if isinstance( obj, list ):
    return { 'SS': obj }
  return None

class toItemException( Exception ):
  '''Exception raised for errors parsing a DynamoDB item to its respective
  object.

  Attributes
  ----------
  itemType = str
    The type of item attempted to parse from.
  '''
  def __init__( self, itemType ):
    self.itemType = itemType
    self.message = f'Could not parse { self.itemType }'
    super().__init__( self.message )
