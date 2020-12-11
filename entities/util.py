def formatDate( date ):
  return date.strftime( '%Y-%m-%dT%H:%M:%S.' ) \
    + date.strftime('%f')[:3] + 'Z' 

def objectToItemAtr( obj ):
  if type( obj ) == str:
    if obj == 'None':
      return { 'NULL': True }
    else: 
      return { 'S': obj }
  if type( obj ) == float or type( obj ) == int:
    return { 'N': str( obj ) }
  if type( obj ) == bool:
    return { 'BOOL': obj }
  if type( obj ) == dict:
    return { 'M': { 
      key: objectToItemAtr( value )
      for key, value in obj.items() 
    } }
  if obj is None:
    return { 'NULL': True }
  else:
    raise Exception( 'Could not parse attribute: ', obj )