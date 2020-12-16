from .util import formatDate, objectToItemAtr, toItemException

class Page:
  '''A class to represent a page item for DynamoDB.

  Attributes
  ----------
  slug : str
    The slug of the page visited.
  title : str
    The title of the page visited.
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

  Methods
  -------
  key():
    Returns the Primary Key of the page.
  gsi1():
    Returns the Primary Key of the first Global Secondary Index of the page.
  gsi1pk():
    Returns the Partition Key of the first Global Secondary Index of the page.
  toItem():
    Returns the page as a parsed DynamoDB item.
  '''
  def __init__(
    self, slug, title, numberVisitors, averageTime, percentChurn, fromPage,
    toPage
  ):
    '''Constructs the necessary attributes for the page object.

    Parameters
    ----------
    slug : str
      The slug of the page visited.
    title : str
      The title of the page visited.
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
    self.slug = slug
    self.title = title
    self.numberVisitors = numberVisitors
    self.averageTime = averageTime
    self.percentChurn = percentChurn
    self.fromPage = fromPage
    self.toPage = toPage

  def key( self ):
    '''Returns the Primary Key of the page.

    This is used to retrieve the unique page from the table.
    '''
    return {
      'PK': { 'S': f'PAGE#{ self.slug }' },
      'SK': { 'S': '#PAGE' }
    }

  def gsi1( self ):
    '''Returns the Primary Key of the first Global Secondary Index of the
    page.

    This is used to retrieve the unique page from the table.
    '''
    return {
      'GSI1PK': { 'S': f'PAGE#{ self.slug }' },
      'GSI1SK': { 'S': f'#PAGE{ formatDate( self.date ) }' }
    }

  def gsi1pk( self ):
    '''Returns the Partition Key of the first Global Secondary Index of the
    visit.

    This is used to retrieve the page-specific data from the table.
    '''
    return { 'S': f'PAGE#{ self.slug }' }

  def toItem( self ):
    '''Returns the visit as a parsed DynamoDB item.

    Returns
    -------
    item : dict
      The visit in DynamoDB syntax.
    '''
    return {
      **self.key(),
      **self.gsi1(),
      'Type': { 'S': 'page' },
      'Title': { 'S': self.title },
      'Slug': { 'S': self.slug },
      'NumberVisitors': objectToItemAtr( self.numberVisitors ),
      'AverageTime': objectToItemAtr( self.averageTime ),
      'PercentChurn': objectToItemAtr( self.percentChurn ),
      'FromPage': objectToItemAtr( self.fromPage ),
      'ToPage': objectToItemAtr( self.toPage )
    }

def itemToPage( item ):
  pass
