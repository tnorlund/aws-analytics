import datetime
from dotenv import load_dotenv
import pytest
load_dotenv()
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
