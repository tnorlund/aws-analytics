import os, sys
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisitor, itemToVisit, itemToSession, \
  itemToLocation, itemToBrowser

def processRecords( records ):
  '''Processes the records for a DynamoDB Stream.

  Parameters
  ----------
  records : list[ dict ]
    The records captured by the DynamoDB stream.

  Returns
  -------
  data : dict
    The captured records parsed as their unique classes.
  '''
  data = {
    'visits': [],
    'browsers': [],
    'sessions': []
  }
  for item in records:
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
      raise Exception( f'Could not parse type: { item }' )
  return data