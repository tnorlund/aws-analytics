import datetime
from .util import objectToItemAtr, formatDate

class Location:
  def __init__( 
    self, ip, country, region, city, latitude, longitude, postalCode, timezone,
    domains,autonomousSystem, isp, proxy, vpn, tor 
  ):
    self.ip = ip
    self.country = country
    self.region = region
    self.city = city
    self.latitude = latitude
    self.longitude = longitude
    self.postalCode = postalCode
    self.timeZone = timezone
    self.domains = domains
    self.autonomousSystem = autonomousSystem
    self.isp = isp
    self.proxy = proxy
    self.vpn = vpn
    self.tor = tor
    self.dateAdded = datetime.datetime.now()

  def key( self ):
    return( {
      'PK': { 'S': f'VISITOR#{ self.ip }' },
      'SK': { 'S': '#LOCATION' }
    } )

  def pk( self ):
    return( {'S': f'VISITOR#{ self.ip }' } )

  def toItem( self ):
    return( {
      **self.key(),
      'Type': { 'S': 'Location' },
      'Country': { 'S': self.country },
      'Region': { 'S': self.region },
      'City': { 'S': self.city },
      'Latitude': { 'N': str( self.latitude ) },
      'Longitude': { 'N': str( self.longitude ) },
      'PostalCode': { 'S': self.postalCode },
      'TimeZone': { 'S': self.timeZone },
      'Domains': { 'SS': self.timeZone },
      'AutonomousSystem': objectToItemAtr( self.autonomousSystem ),
      'ISP': { 'S': self.isp },
      'Proxy': { 'BOOL': self.proxy },
      'VPN': { 'BOOL': self.vpn },
      'TOR': { 'BOOL': self.tor },
      'DateAdded': { 'S': formatDate( self.dateAdded ) }
    } )

  def __repr__( self ):
    return f"{ self.ip } - { self.city }"
  
  def __iter__( self ):
    yield 'ip', self.ip
    yield 'country', self.country
    yield 'region', self.region
    yield 'city', self.city
    yield 'lat', self.latitude
    yield 'lng', self.longitude
    yield 'postalCode', self.postalCode
    yield 'timezone', self.timeZone
    yield 'domains', self.domains
    yield 'as', self.autonomousSystem
    yield 'isp', self.isp
    yield 'proxy', self.proxy
    yield 'vpn', self.vpn
    yield 'tor', self.tor
    yield 'dateAdded', self.dateAdded

def requestToLocation( req ):
  return Location(
    req['ip'], req['location']['country'], req['location']['region'],
    req['location']['city'], req['location']['lat'], req['location']['lng'],
    req['location']['postalCode'], req['location']['timezone'], req['domains'],
    req['as'] if 'as' in req else None, req['isp'], req['proxy']['proxy'], 
    req['proxy']['vpn'], req['proxy']['tor'] 
  )