class Visitor:
  def __init__( self, ip, numberSessions = 0 ):
    self.ip = ip
    self.numberSessions = numberSessions

  def key( self ):
    return( {
      'PK': { 'S': f'VISITOR#{ self.ip }' },
      'SK': { 'S': '#VISITOR' }
    } )
  
  def pk( self ):
    return( { 'S': f'VISITOR#{ self.ip }' } )

  def toItem( self ):
    return( {
      **self.key(),
      'Type': { 'S': 'visitor' },
      'NumberSessions': { 'N': str( self.numberSessions ) }
    } )

  def __repr__( self ):
    return f"{ self.ip } - { self.numberSessions }"

def itemToVisitor( item ):
  return Visitor( 
    item['PK']['S'].split('#')[1], 
    int( item['NumberSessions']['N'] ) 
  )