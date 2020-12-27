# pylint: disable=redefined-outer-name, unused-argument
import datetime
import pytest
from dynamo.entities import Browser, itemToBrowser # pylint: disable=wrong-import-position

@pytest.fixture
def ip():
  return '0.0.0.0'

@pytest.fixture
def pixel_app():
  return 'Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 ' + \
    '(KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36'

@pytest.fixture
def mac_chrome_app():
  return 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) ' + \
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'

@pytest.fixture
def mac_safari_app():
  return 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) ' + \
    'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15'

@pytest.fixture
def windows_chrome_app():
  return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' + \
    '(KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'

@pytest.fixture
def iphone_safari_app():
  return 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) ' + \
    'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Mobile/15E148 ' + \
    'Safari/604.1'

@pytest.fixture
def iphone_linkedin_app():
  return 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) ' + \
    'AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [LinkedInApp]'

def test_default_android_init( ip, pixel_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    pixel_app, ip, 100, 200, '2020-01-01T00:00:00.000Z', dateAdded = currentTime
  )
  assert browser.app == pixel_app
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device == 'Pixel 4 XL'
  assert browser.deviceType == 'mobile'
  assert browser.browser == 'chrome'
  assert browser.os == '11'
  assert browser.webkit == '537.36'
  assert browser.version == '86.0.4240.198'

def test_default_mac_chrome_init( ip, mac_chrome_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    mac_chrome_app, ip, 100, 200, '2020-01-01T00:00:00.000Z',
    dateAdded = currentTime
  )
  assert browser.app == mac_chrome_app
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device == 'mac'
  assert browser.deviceType == 'desktop'
  assert browser.browser == 'chrome'
  assert browser.os == '11.1.0'
  assert browser.webkit == '537.36'
  assert browser.version == '87.0.4280.88'

def test_default_mac_safari_init( ip, mac_safari_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    mac_safari_app, ip, 100, 200, '2020-01-01T00:00:00.000Z',
    dateAdded = currentTime
  )
  assert browser.app == mac_safari_app
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device == 'mac'
  assert browser.deviceType == 'desktop'
  assert browser.browser == 'safari'
  assert browser.os == '10.15.6'
  assert browser.webkit == '605.1.15'
  assert browser.version == '14.0.2'

def test_default_windows_chrome_init( ip, windows_chrome_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    windows_chrome_app, ip, 100, 200, '2020-01-01T00:00:00.000Z',
    dateAdded = currentTime
  )
  assert browser.app == windows_chrome_app
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device == 'windows'
  assert browser.deviceType == 'desktop'
  assert browser.browser == 'chrome'
  assert browser.os == '10.0'
  assert browser.webkit == '537.36'
  assert browser.version == '87.0.4280.88'

def test_default_iphone_safari_init( ip, iphone_safari_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    iphone_safari_app, ip, 100, 200, '2020-01-01T00:00:00.000Z',
    dateAdded = currentTime
  )
  assert browser.app == iphone_safari_app
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device == 'iphone'
  assert browser.deviceType == 'mobile'
  assert browser.browser == 'safari'
  assert browser.os == '14.3'
  assert browser.webkit == '605.1.15'
  assert browser.version == '14.0.2'

def test_default_iphone_linkedin_init( ip, iphone_linkedin_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    iphone_linkedin_app, ip, 100, 200, '2020-01-01T00:00:00.000Z',
    dateAdded = currentTime
  )
  assert browser.app == iphone_linkedin_app
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device == 'iphone'
  assert browser.deviceType == 'mobile'
  assert browser.browser == '[LinkedInApp]'
  assert browser.os == '14.2'
  assert browser.webkit == '605.1.15'
  assert browser.version is None

def test_default_unknown_init( ip ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    'unknown', ip, 100, 200, '2020-01-01T00:00:00.000Z', dateAdded = currentTime
  )
  assert browser.app == 'unknown'
  assert browser.ip == ip
  assert browser.width == 100
  assert browser.height == 200
  assert browser.dateVisited == datetime.datetime( 2020, 1, 1, 0, 0, 0 )
  assert browser.dateAdded == currentTime
  assert browser.device is None
  assert browser.deviceType is None
  assert browser.browser is None
  assert browser.os is None
  assert browser.webkit is None
  assert browser.version is None

def test_key( ip, pixel_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    pixel_app, ip, 100, 200, '2020-01-01T00:00:00.000Z', dateAdded = currentTime
  )
  assert browser.key() == {
    'PK': { 'S': f'VISITOR#{ ip }' },
    'SK': { 'S': 'BROWSER#2020-01-01T00:00:00.000Z' }
  }

def test_pk( ip, pixel_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    pixel_app, ip, 100, 200, '2020-01-01T00:00:00.000Z', dateAdded = currentTime
  )
  assert browser.pk() == { 'S': f'VISITOR#{ ip }' }

def test_toItem( ip, pixel_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    pixel_app, ip, 100, 200, '2020-01-01T00:00:00.000Z', dateAdded = currentTime
  )
  assert browser.toItem() == {
    'PK': { 'S': f'VISITOR#{ ip }' },
    'SK': { 'S': 'BROWSER#2020-01-01T00:00:00.000Z' },
    'Type': { 'S': 'browser' },
    'App': { 'S': pixel_app },
    'Width': { 'N': '100' },
    'Height': { 'N': '200' },
    'DateVisited': { 'S': '2020-01-01T00:00:00.000Z' },
    'Device': { 'S': 'Pixel 4 XL' },
    'DeviceType': { 'S': 'mobile' },
    'Browser': { 'S': 'chrome' },
    'OS': { 'S': '11' },
    'Webkit': { 'S': '537.36' },
    'Version': { 'S': '86.0.4240.198' },
    'DateAdded': { 'S': currentTime.strftime( '%Y-%m-%dT%H:%M:%S.' ) \
      + currentTime.strftime('%f')[:3] + 'Z' }
  }

def test_repr( ip, pixel_app ):
  assert repr( Browser(
    pixel_app, ip, 100, 200, '2020-01-01T00:00:00.000Z'
  ) ) == f'{ ip } - chrome'

def test_itemToBrowser( ip, pixel_app ):
  currentTime = datetime.datetime.now()
  browser = Browser(
    pixel_app, ip, 100, 200, '2020-01-01T00:00:00.000Z', 
    dateAdded = currentTime.strftime( '%Y-%m-%dT%H:%M:%S.' ) \
      + currentTime.strftime('%f')[:3] + 'Z'
  )
  newBrowser = itemToBrowser( browser.toItem() )
  assert browser.app == newBrowser.app
  assert browser.ip == newBrowser.ip
  assert browser.width == newBrowser.width
  assert browser.height == newBrowser.height
  assert browser.dateVisited == newBrowser.dateVisited
  assert browser.dateAdded == newBrowser.dateAdded
  assert browser.device == newBrowser.device
  assert browser.deviceType == newBrowser.deviceType
  assert browser.browser == newBrowser.browser
  assert browser.os == newBrowser.os
  assert browser.webkit == newBrowser.webkit
  assert browser.version == newBrowser.version

def test_itemToBrowser_exception():
  with pytest.raises( Exception ) as e:
    assert itemToBrowser( {} )
  assert str( e.value ) == "Could not parse browser"
