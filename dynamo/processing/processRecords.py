import os
import sys
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import itemToVisitor, itemToVisit, itemToSession # pylint: disable=wrong-import-position
from dynamo.entities import itemToLocation, itemToBrowser # pylint: disable=wrong-import-position

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
    'visits': [], 'browsers': [], 'sessions': [], 'locations': [],
    'visitors': []
  }
  for item in records:
    if item['Type']['S'] == 'visitor':
      data['visitors'].append( itemToVisitor( item ) )
    elif item['Type']['S'] == 'visit':
      data['visits'].append( itemToVisit( item ) )
    elif item['Type']['S'] == 'session':
      data['sessions'].append( itemToSession( item ) )
    elif item['Type']['S'] == 'location':
      data['locations'].append( itemToLocation( item ) )
    elif item['Type']['S'] == 'browser':
      data['browsers'].append( itemToBrowser( item ) )
  return data
