import re
import datetime
from .util import objectToItemAtr, formatDate

class Browser:
  def __init__(
    self, app, ip, width, height, dateVisited, device = None,
    deviceType = None, browser = None, os = None,  webkit = None,
    version = None, dateAdded = datetime.datetime.now()
  ):
    self.width = width
    self.height = height
    self.dateVisited = datetime.datetime.strptime(
      dateVisited, '%Y-%m-%dT%H:%M:%S.%fZ'
    )
    # Mac - Safari
    if re.match(
      r"Mozilla\/5\.0 \(Macintosh; Intel Mac OS X (\d+_\d+_\d+)\) " + \
      r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Version\/" + \
      r"(\d+\.\d+\.\d+) Safari\/(\d+\.\d+\.\d+)",
      app
    ):
      match = re.match(
        r"Mozilla\/5\.0 \(Macintosh; Intel Mac OS X (\d+_\d+_\d+)\) "  + \
        r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Version\/" + \
        r"(\d+\.\d+\.\d+) Safari\/(\d+\.\d+\.\d+)",
        app
      )
      self.app = app
      self.device = 'mac'
      self.type = 'desktop'
      self.browser = 'safari'
      self.os = match.group(1).replace( '_', '.' )
      self.webkit = match.group(2)
      self.version = match.group(3)
      self.ip = ip
      self.dateAdded = dateAdded
    # Mac - Chrome
    elif re.match(
      r"Mozilla\/5\.0 \(Macintosh; Intel Mac OS X (\d+_\d+_\d+)\) " + \
      r"AppleWebKit\/(\d+\.\d+) \(KHTML, like Gecko\) Chrome\/" + \
      r"(\d+\.\d+\.\d+\.\d+) Safari\/(\d+\.\d+)",
      app
    ):
      match = re.match(
        r"Mozilla\/5\.0 \(Macintosh; Intel Mac OS X (\d+_\d+_\d+)\) " + \
        r"AppleWebKit\/(\d+\.\d+) \(KHTML, like Gecko\) Chrome\/" + \
        r"(\d+\.\d+\.\d+\.\d+) Safari\/(\d+\.\d+)",
        app
      )
      self.app = app
      self.device = 'mac'
      self.type = 'desktop'
      self.browser = 'chrome'
      self.os = match.group(1).replace( '_', '.' )
      self.webkit = match.group(2)
      self.version = match.group(3)
      self.ip = ip
      self.dateAdded = dateAdded
    # Windows - Chrome
    elif re.match(
      r"Mozilla\/5\.0 \(Windows NT (\d+\.\d+); Win64; x64\) AppleWebKit" + \
      r"\/(\d+\.\d+) \(KHTML, like Gecko\) Chrome\/(\d+\.\d+\.\d+\.\d+) " + \
      r"Safari\/(\d+\.\d+)",
      app
    ):
      match = re.match(
        r"Mozilla\/5\.0 \(Windows NT (\d+\.\d+); Win64; x64\) AppleWebKit" + \
        r"\/(\d+\.\d+) \(KHTML, like Gecko\) Chrome\/(\d+\.\d+\.\d+\.\d+) " + \
        r"Safari\/(\d+\.\d+)",
        app
      )
      self.app = app
      self.device = 'windows'
      self.type = 'desktop'
      self.browser = 'chrome'
      self.os = match.group(1)
      self.webkit = match.group(2)
      self.version = match.group(3)
      self.ip = ip
      self.dateAdded = dateAdded
    # iPhone - Safari
    elif re.match(
      r"Mozilla\/5\.0 \(iPhone; CPU iPhone OS (\d+_\d+) like Mac OS X\) " + \
      r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Version\/" + \
      r"(\d+\.\d+\.\d+|\d+\.\d) Mobile\/15E148 Safari\/(\d+\.\d+)",
      app
    ):
      match = re.match(
        r"Mozilla\/5\.0 \(iPhone; CPU iPhone OS (\d+_\d+) like Mac OS X\) " + \
        r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Version\/" + \
        r"(\d+\.\d+\.\d+|\d+\.\d) Mobile\/15E148 Safari\/(\d+\.\d+)",
        app
      )
      self.app = app
      self.device = 'iphone'
      self.type = 'mobile'
      self.browser = 'safari'
      self.os = match.group(1).replace( '_', '.' )
      self.webkit = match.group(2)
      self.version = match.group(3)
      self.ip = ip
      self.dateAdded = dateAdded
    # iPhone - App
    elif re.match(
      r"Mozilla\/5\.0 \(iPhone; CPU iPhone OS (\d+_\d+) like Mac OS X\) " + \
      r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Mobile\/15E148 " + \
      r"(\[[a-zA-Z0-9]+\])",
      app
    ):
      match = re.match(
        r"Mozilla\/5\.0 \(iPhone; CPU iPhone OS (\d+_\d+) like Mac OS X\) " + \
        r"AppleWebKit\/(\d+\.\d+\.\d+) \(KHTML, like Gecko\) Mobile\/" + \
        r"15E148 (\[[a-zA-Z0-9]+\])",
        app
      )
      self.app = app
      self.device = 'iphone'
      self.type = 'mobile'
      self.browser = match.group(3)
      self.os = match.group(1).replace( '_', '.' )
      self.webkit = match.group(2)
      self.version = None
      self.ip = ip
      self.dateAdded = dateAdded
    else:
      self.app = app
      self.device = device
      self.type = deviceType
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
      'DeviceType': objectToItemAtr( self.type ),
      'Browser': objectToItemAtr( self.browser ),
      'OS': objectToItemAtr( self.os ),
      'Webkit': objectToItemAtr( self.webkit ),
      'Version': objectToItemAtr( self.version ),
      'DateAdded': objectToItemAtr( formatDate( self.dateAdded ) )
    } )

  def __repr__( self ):
    return f"{ self.ip } - { self.browser }"

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
