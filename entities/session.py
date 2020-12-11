from .util import formatDate
import datetime

class Session:
  """Session item for DynamoDB."""
  def __init__(
    self, sessionStart, ip, avgTime, totalTime
  ):
    self.sessionStart = datetime.datetime.strptime( sessionStart, '%Y-%m-%dT%H:%M:%S.%fZ' ) if type( sessionStart ) == str else sessionStart
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
      'AverageTime': { 'N': str( self.avgTime ) },
      'TotalTime': { 'N': str( self.totalTime ) }
    } )

  def __repr__( self ):
    return( f'{ self.ip } - { self.totalTime }' )