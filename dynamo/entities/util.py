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
  if isinstance( obj, str ):
    if obj == 'None':
      return { 'NULL': True }
    return { 'S': obj }
  if isinstance( obj, ( float, int ) ):
    return { 'N': str( obj ) }
  if isinstance( obj, bool ):
    return { 'BOOL': obj }
  if isinstance( obj, dict ):
    return { 'M': {
      key: objectToItemAtr( value )
      for key, value in obj.items()
    } }
  if obj is None:
    return { 'NULL': True }
  raise Exception( 'Could not parse attribute: ', obj )

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
