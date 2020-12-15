import datetime
import numpy as np
from .util import formatDate, objectToItemAtr
class Visit:
  """Visit item for DynamoDB.
  """
  def __init__(
    self, date, ip, user, title, slug, sessionStart, timeOnPage=None,
    prevTitle=None, prevSlug=None, nextTitle=None, nextSlug=None,
  ):
    self.date = datetime.datetime.strptime(
      date, '%Y-%m-%dT%H:%M:%S.%fZ'
    ) if type( date ) == str else date
    self.ip = ip
    if user == 'None' or user is None:
      self.user = 0
    else:
      self.user = int( user )
    self.title = title
    self.slug = slug
    self.sessionStart = datetime.datetime.strptime( 
      sessionStart, '%Y-%m-%dT%H:%M:%S.%fZ' 
    ) if type( sessionStart ) == str else sessionStart
    self.prevTitle = prevTitle
    self.prevSlug = prevSlug
    self.nextTitle = nextTitle
    self.nextSlug = nextSlug
    self.timeOnPage = timeOnPage if ~np.isnan( timeOnPage ) else None

  def key( self ):
    return( {
      'PK': { 'S': f'VISITOR#{ self.ip }' },
      'SK': { 'S': f'VISIT#{ formatDate( self.date ) }#{ self.slug }' }
    } )

  def pk( self ):
    return( { 'S': f'VISITOR#{ self.ip }' } )

  def gsi1( self ):
    return( {
      'GSI1PK': { 'S': f'PAGE#{ self.slug }' },
      'GSI1SK': { 'S': f'VISIT#{ formatDate( self.date ) }' }
    } )

  def gsi2( self ):
    return( {
      'GSI2PK': { 'S': f'''SESSION#{
        self.ip 
      }#{ formatDate( self.sessionStart ) }''' },
      'GSI2SK': { 'S': f'VISIT#{ formatDate( self.date ) }' }
    } )

  def toItem( self ):
    """Converts the visit into a DynamoDB item."""
    return( {
      **self.key(),
      **self.gsi1(),
      **self.gsi2(),
      'Type': { 'S': 'visit' },
      'User': { 'N': f'{self.user}' },
      'Title': { 'S': self.title },
      'Slug': { 'S': self.slug },
      'PreviousTitle': objectToItemAtr( self.prevTitle ),
      'PreviousSlug': objectToItemAtr( self.prevSlug ),
      'NextTitle': objectToItemAtr( self.nextTitle ),
      'NextSlug': objectToItemAtr( self.nextSlug ),
      'TimeOnPage': objectToItemAtr( self.timeOnPage )
    } )

  def __repr__( self ):
    return f"{ self.ip } - { formatDate( self.date ) }"

  def __iter__( self ):
    yield 'date', self.date
    yield 'ip', self.ip
    yield 'user', self.user
    yield 'title', self.title
    yield 'slug', self.slug
    yield 'prevTitle', self.prevTitle
    yield 'prevSlug', self.prevSlug
    yield 'nextTitle', self.nextTitle
    yield 'nextSlug', self.nextSlug
    yield 'timeOnPage', self.timeOnPage

def itemToVisit( item ):
  return Visit(
    item['SK']['S'].split('#')[1], item['PK']['S'].split('#')[1], 
    int( item['User']['N'] ), item['Title']['S'], item['Slug']['S'],
    item['GSI2PK']['S'].split('#')[2], 
    np.nan  
      if 'NULL' in item['TimeOnPage'] 
      else float( item['TimeOnPage']['N'] ),
    None 
      if 'NULL' in item['PreviousTitle'].keys()
      else  item['PreviousTitle']['S'],
    None 
      if 'NULL' in item['PreviousSlug'].keys() 
      else  item['PreviousSlug']['S'],
    None 
      if 'NULL' in item['NextTitle'].keys() 
      else  item['NextTitle']['S'],
    None 
      if 'NULL' in item['NextSlug'].keys() 
      else  item['NextSlug']['S']
  )
