import datetime
from dotenv import load_dotenv
import pytest
load_dotenv()
from dynamo.entities import Location, requestToLocation, itemToLocation # pylint: disable=wrong-import-position

def test_init():
  currentTime = datetime.datetime.now()
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False, currentTime
  )
  assert location.ip == '0.0.0.0'
  assert location.country == 'US'
  assert location.region == 'California'
  assert location.city == 'Westlake Village'
  assert location.latitude == 34.141944
  assert location.longitude == -118.819444
  assert location.postalCode == '91361'
  assert location.timeZone == '-08:00'
  assert location.domains == ['cpe-75-82-84-171.socal.res.rr.com']
  assert location.autonomousSystem == {
    'asn': 20001,
    'name': 'Charter Communications (20001)',
    'route': '75.82.0.0/15',
    'domain': 'https://www.spectrum.com',
    'type': 'Cable/DSL/ISP'
  }
  assert location.isp == 'Charter Communications'
  assert not location.proxy
  assert not location.vpn
  assert not location.tor
  assert location.dateAdded == currentTime

def test_key():
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False
  )
  assert location.key() == {
    'PK': { 'S': 'VISITOR#0.0.0.0' },
    'SK': { 'S': '#LOCATION' }
  }

def test_pk():
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False
  )
  assert location.pk() == { 'S': 'VISITOR#0.0.0.0' }

def test_toItem():
  currentTime = datetime.datetime.now()
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False, currentTime
  )
  assert location.toItem() == {
    'PK': { 'S': 'VISITOR#0.0.0.0' },
    'SK': { 'S': '#LOCATION' },
    'Type': { 'S': 'location' },
    'Country': { 'S': 'US' },
    'Region': { 'S': 'California' },
    'City': { 'S': 'Westlake Village' },
    'Latitude': { 'N': '34.141944' },
    'Longitude': { 'N': '-118.819444' },
    'PostalCode': { 'S': '91361' },
    'TimeZone': { 'S': '-08:00' },
    'Domains': { 'SS': ['cpe-75-82-84-171.socal.res.rr.com'] },
    'AutonomousSystem': { 
      'M': {
        'asn': { 'N': '20001' },
        'name': { 'S': 'Charter Communications (20001)' },
        'route': { 'S': '75.82.0.0/15' },
        'domain': { 'S': 'https://www.spectrum.com' },
        'type': { 'S': 'Cable/DSL/ISP' }
      }
    },
    'ISP': { 'S': 'Charter Communications' },
    'Proxy': { 'BOOL': False },
    'VPN': { 'BOOL': False },
    'TOR': { 'BOOL': False },
    'DateAdded': { 'S': currentTime.strftime( '%Y-%m-%dT%H:%M:%S.' ) \
      + currentTime.strftime('%f')[:3] + 'Z' } 
  }

def test_repr():
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False
  )
  assert repr( location ) == '0.0.0.0 - Westlake Village'

def test_dict():
  currentTime = datetime.datetime.now()
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False, currentTime
  )
  assert dict( location ) == {
    'ip': '0.0.0.0',
    'country': 'US',
    'region': 'California',
    'city': 'Westlake Village',
    'lat': 34.141944,
    'lng': -118.819444,
    'postalCode': '91361',
    'timezone': '-08:00',
    'domains': ['cpe-75-82-84-171.socal.res.rr.com'],
    'as': {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    },
    'isp': 'Charter Communications',
    'proxy': False,
    'vpn': False,
    'tor': False,
    'dateAdded': currentTime
  }

def test_requestToLocation():
  location = requestToLocation(
    {
      'ip': '0.0.0.0',
      'location': {
        'country': 'US',
        'region': 'California',
        'city': 'Westlake Village',
        'lat': 34.14584,
        'lng': -118.80565,
        'postalCode': '',
        'timezone': '-08:00',
        'geonameId': 5408395
      },
      'domains': ['cpe-75-82-84-171.socal.res.rr.com'],
      'as': {
        'asn': 20001,
        'name': 'Charter Communications (20001)',
        'route': '75.82.0.0/15',
        'domain': 'https://www.spectrum.com',
        'type': 'Cable/DSL/ISP'
      },
      'isp': 'Charter Communications',
      'proxy': { 'proxy': False, 'vpn': False, 'tor': False }
    }
  )
  assert location.ip == '0.0.0.0'
  assert location.country == 'US'
  assert location.region == 'California'
  assert location.city == 'Westlake Village'
  assert location.latitude == 34.14584
  assert location.longitude == -118.80565
  assert location.postalCode == ''
  assert location.timeZone == '-08:00'
  assert location.domains == ['cpe-75-82-84-171.socal.res.rr.com']
  assert location.autonomousSystem == {
    'asn': 20001,
    'name': 'Charter Communications (20001)',
    'route': '75.82.0.0/15',
    'domain': 'https://www.spectrum.com',
    'type': 'Cable/DSL/ISP'
  }
  assert location.isp == 'Charter Communications'
  assert not location.proxy
  assert not location.vpn
  assert not location.tor

def test_exception_requestToLocation():
  with pytest.raises( Exception ) as e:
    assert requestToLocation( {} )
  assert str( e.value ) == 'Could not parse location'

def test_itemToLocation():
  location = Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'], {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False
  )
  newLocation = itemToLocation( location.toItem() )
  assert location.ip == newLocation.ip
  assert location.country == newLocation.country
  assert location.region == newLocation.region
  assert location.city == newLocation.city
  assert location.latitude == newLocation.latitude
  assert location.longitude == newLocation.longitude
  assert location.postalCode == newLocation.postalCode
  assert location.timeZone == newLocation.timeZone
  assert location.domains == newLocation.domains
  assert location.autonomousSystem == newLocation.autonomousSystem
  assert location.isp == newLocation.isp
  assert location.proxy == newLocation.proxy
  assert location.vpn == newLocation.vpn
  assert location.tor == newLocation.tor

def test_itemToLocation_exception():
  with pytest.raises( Exception ) as e:
    assert itemToLocation( {} )
  assert str( e.value ) == "Could not parse location"
