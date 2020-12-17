import re
import datetime
from .util import objectToItemAtr, formatDate

# TODO
# [ ] Refactor browser initialization to use multiple functions
# [ ] Add docstrings

class Browser:
  '''A class to represent a browser item for DynamoDB.
  '''
  def __init__(
    self, app, ip, width, height, dateVisited, device = None,
    deviceType = None, browser = None, os = None,  webkit = None,
    version = None, dateAdded = datetime.datetime.now()
  ):
    self.app = app
    self.ip = ip
    self.width = width
    self.height = height
    self.dateVisited = datetime.datetime.strptime(
      dateVisited, '%Y-%m-%dT%H:%M:%S.%fZ'
    )
    self.dateAdded = dateAdded
    result = self._matchMac( app )
    if not result:
      result = self._matchWindows( app )
    elif not result:
      result = self._matchiPhone( app )
    elif not result:
      self.app = app
      self.device = device
      self.deviceType = deviceType
      self.browser = browser
      self.os = os
      self.webkit = webkit
      self.version = version
      self.ip = ip
      self.dateAdded = dateAdded

  def key( self ):
    return( {
      'PK': { 'S': f'VISITOR#{ self.ip }' },
      'SK': { 'S': f'VISIT#{ formatDate( self.dateVisited ) }' }
    } )

  def pk( self ):
    return { 'S': f'VISITOR#{ self.ip }' }

  def toItem( self ):
    return( {
      **self.key(),
      'Type': { 'S': 'browser' },
      'App': objectToItemAtr( self.app ),
      'Width': objectToItemAtr( self.width ),
      'Height': objectToItemAtr( self.height ),
      'DateVisited': {'S': formatDate( self.dateVisited ) },
      'Device': objectToItemAtr( self.device ),
      'DeviceType': objectToItemAtr( self.deviceType ),
      'Browser': objectToItemAtr( self.browser ),
      'OS': objectToItemAtr( self.os ),
      'Webkit': objectToItemAtr( self.webkit ),
      'Version': objectToItemAtr( self.version ),
      'DateAdded': objectToItemAtr( formatDate( self.dateAdded ) )
    } )

  def __repr__( self ):
    return f"{ self.ip } - { self.browser }"

  def _matchMac( self, app ):
    # Mac - Safari
    safari_match = re.match(
      r"Mozilla\/5\.0 \(Macintosh; Intel Mac OS X (\d+_\d+_\d+)\) "  + \
      r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Version\/" + \
      r"(\d+\.\d+\.\d+) Safari\/(\d+\.\d+\.\d+)",
      app
    )
    # Mac - Chrome
    chrome_match = re.match(
      r"Mozilla\/5\.0 \(Macintosh; Intel Mac OS X (\d+_\d+_\d+)\) " + \
      r"AppleWebKit\/(\d+\.\d+) \(KHTML, like Gecko\) Chrome\/" + \
      r"(\d+\.\d+\.\d+\.\d+) Safari\/(\d+\.\d+)",
      app
    )
    if safari_match:
      self.device = 'mac'
      self.deviceType = 'desktop'
      self.browser = 'safari'
      self.os = safari_match.group( 1 ).replace( '_', '.' )
      self.webkit = safari_match.group( 2 )
      self.version =  safari_match.group( 3 )
      return True
    if chrome_match:
      self.device = 'mac'
      self.deviceType = 'desktop'
      self.browser = 'chrome'
      self.os = safari_match.group( 1 ).replace( '_', '.' )
      self.webkit = safari_match.group( 2 )
      self.version =  safari_match.group( 3 )
      return True
    return False

  def _matchWindows( self, app ):
    # Windows - Chrome
    match = re.match(
      r"Mozilla\/5\.0 \(Windows NT (\d+\.\d+); Win64; x64\) AppleWebKit" + \
      r"\/(\d+\.\d+) \(KHTML, like Gecko\) Chrome\/(\d+\.\d+\.\d+\.\d+) " + \
      r"Safari\/(\d+\.\d+)",
      app
    )
    if match:
      self.device = 'windows'
      self.deviceType = 'desktop'
      self.browser = 'chrome'
      self.os = match.group( 1 ).replace( '_', '.' )
      self.webkit = match.group( 2 )
      self.version = match.group( 3 )
      return True
    return False

  def _matchiPhone( self, app ):
    # iPhone - Safari
    iphone_match = re.match(
      r"Mozilla\/5\.0 \(iPhone; CPU iPhone OS (\d+_\d+) like Mac OS X\) " + \
      r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Version\/" + \
      r"(\d+\.\d+\.\d+|\d+\.\d) Mobile\/15E148 Safari\/(\d+\.\d+)",
      app
    )
    # iPhone - App
    app_match = re.match(
      r"Mozilla\/5\.0 \(iPhone; CPU iPhone OS (\d+_\d+) like Mac OS X\) " + \
      r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Mobile\/15E148 " + \
      r"(\[[a-zA-Z0-9]+\])",
      app
    )
    if iphone_match:
      self.device = 'iphone'
      self.deviceType = 'mobile'
      self.browser = 'safari'
      self.os = iphone_match.group(1).replace( '_', '.' )
      self.webkit = iphone_match.group(2)
      self.version = iphone_match.group(3)
      return True
    if app_match:
      self.device = 'iphone'
      self.deviceType = 'mobile'
      self.browser = app_match.group(3)
      self.os = app_match.group(1).replace( '_', '.' )
      self.webkit = app_match.group(2)
      self.version = None
      return True
    return False

def itemToBrowser( item ):
  return Browser(
    item['App']['S'], item['PK']['S'].split('#')[1], item['Width']['N'],
    item['Height']['N'], item['DateVisited']['S'],
    None if 'NULL' in item['Device'].keys() else item['Device']['S'],
    None if 'NULL' in item['DeviceType'].keys() else item['DeviceType']['S'],
    None if 'NULL' in item['Browser'].keys() else item['Browser']['S'],
    None if 'NULL' in item['OS'].keys() else item['OS']['S'],
    None if 'NULL' in item['Webkit'].keys() else item['Webkit']['S'],
    None if 'NULL' in item['Version'].keys() else item['Version']['S'],
    datetime.datetime.strptime(
      item['SK']['S'].split('#')[1], '%Y-%m-%dT%H:%M:%S.%fZ'
    )
  )
