from .util import formatDate
import datetime

class Session:
  """Session item for DynamoDB."""
  def __init__(
    self, sessionStart, ip, avgTime, totalTime
  ):
    self.sessionStart = sessionStart \
      if type( sessionStart ) == datetime.datetime \
      else datetime.datetime.strptime(
        sessionStart, '%Y-%m-%dT%H:%M:%S.%fZ'
      )
    self.ip = ip
    self.avgTime = avgTime
    self.totalTime = totalTime
  
  def key( self ): 
    return( {
      'PK': { 'S': f'VISITOR#{ self.ip }' },
      'SK': { 'S': f'SESSION#{ formatDate( self.sessionStart ) }' }
    } )
  
  def pk( self ):
    return( { 'S': f'VISITOR#{ self.ip }' } )
  
  def gsi2( self ):
    return( {
      'GSI2PK': { 'S': f'''SESSION#{
        self.ip 
      }#{ formatDate( self.sessionStart ) }''' },
      'GSI2SK': { 'S': f'#SESSION' }
    } )
  
  def toItem( self ):
    return( {
      **self.key(),
      **self.gsi2(),
      'Type': { 'S': 'session' },
      'AverageTime': { 'N': str( self.avgTime ) },
      'TotalTime': { 'N': str( self.totalTime ) }
    } )

  def __repr__( self ):
    return( f'{ self.ip } - { self.totalTime }' )

def itemToSession( item ):
  return Session(
    datetime.datetime.strptime(
      item['SK']['S'].split('#')[1], '%Y-%m-%dT%H:%M:%S.%fZ'
    ), item['PK']['S'].split('#')[1], 
    float( item['AverageTime']['N'] ),
    float( item['TotalTime']['N'] )
  )