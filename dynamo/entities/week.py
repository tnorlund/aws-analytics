import re
from .util import objectToItemAtr, toItemException

class Week:
  def __init__(
    self, slug, title, date, numberVisitors, averageTime,
    percentChurn, fromPage, toPage
  ):
    '''Constructs the necessary attributes for the week object.

    Parameters
    ----------
    slug : str
      The slug of the page visited.
    title : str
      The title of the page visited.
    date : str
      The week's date in the format "<year>-<week>".
    numberVisitors : int
      The number of page's unique visitors.
    averageTime : float
      The average number of time spent on the page until going to another page
      on the website.
    percentChurn : float
      The percentage of the visitors that churned on this page rather than
      continuing to visit other pages.
    fromPage : dict
      The different pages the visitors came from and their ratios.
    toPage : dict
      The different pages the visitors when to and their ratios.
    '''
    dateMatch = re.match( r'(\d+)-(\d+)', date )
    if not dateMatch:
      raise ValueError( 'Must give month as "<year>-<month>-<day>"' )
    if len( dateMatch.group( 1 ) ) != 4:
      raise ValueError( 'Must give valid year' )
    if int( dateMatch.group( 2 ) ) < 0 or int( dateMatch.group(2) ) > 52:
      raise ValueError( 'Must give valid week' )
    self.slug = slug
    self.title = title
    self.year = int( dateMatch.group( 1 ) )
    self.week = int( dateMatch.group( 2 ) )
    self.numberVisitors = numberVisitors
    self.averageTime = averageTime
    self.percentChurn = percentChurn
    self.fromPage = fromPage
    self.toPage = toPage

  def key( self ):
    '''Returns the Primary Key of the week.

    This is used to retrieve the unique week from the table.
    '''
    return {
      'PK': { 'S': f'PAGE#{ self.slug }' },
      'SK': { 'S': f'#WEEK#{ self.year }-{ self.week:02 }' }
    }

  def gsi1( self ):
    '''Returns the Primary Key of the first Global Secondary Index of the
    day.

    This is used to retrieve the unique week from the table.
    '''
    return {
      'GSI1PK': { 'S': f'PAGE#{ self.slug }' },
      'GSI1SK': {
        'S': f'#WEEK#{ self.year }-{ self.week:02 }'
      }
    }

  def gsi1pk( self ):
    '''Returns the Partition Key of the first Global Secondary Index of the
    visit.

    This is used to retrieve the page-specific data from the table.
    '''
    return { 'S': f'PAGE#{ self.slug }' }

  def toItem( self ):
    '''Returns the week as a parsed DynamoDB item.

    Returns
    -------
    item : dict
      The week in DynamoDB syntax.
    '''
    return {
      **self.key(),
      **self.gsi1(),
      'Type': { 'S': 'day' },
      'Title': { 'S': self.title },
      'Slug': { 'S': self.slug },
      'NumberVisitors': objectToItemAtr( self.numberVisitors ),
      'AverageTime': objectToItemAtr( self.averageTime ),
      'PercentChurn': objectToItemAtr( self.percentChurn ),
      'FromPage': objectToItemAtr( self.fromPage ),
      'ToPage': objectToItemAtr( self.toPage )
    }

  def __repr__( self ):
    return f'{ self.title }-{ self.year }/{ self.week:02 }'

def itemToWeek( item ):
  '''Parses a DynamoDB item as a week object.

  Parameters
  ----------
  item : dict
    The raw DynamoDB item.

  Raises
  ------
  toItemException
    When the item is missing the required keys to parse into a week object.

  Returns
  -------
  week : Week
    The week object parsed from the raw DynamoDB item.
  '''
  try:
    return Week(
      item['Slug']['S'], item['Title']['S'], item['SK']['S'].split('#')[2],
      item['NumberVisitors']['N'], item['AverageTime']['N'],
      item['PercentChurn']['N'],
      {
        key: float( value['N'] )
        for (key, value) in item['FromPage']['M'].items()
        if 'N' in value.keys()
      },
      {
        key: float( value['N'] )
        for (key, value) in item['ToPage']['M'].items()
        if 'N' in value.keys()
      }
    )
  except Exception as e:
    print( f'ERROR itemToDay: {e}' )
    raise toItemException( 'day' ) from e
