# pylint: disable=redefined-outer-name, unused-argument
import pytest
import numpy as np
from dynamo.entities import Session, Visit, Visitor, Browser # pylint: disable=wrong-import-position
from dynamo.entities import Year, Month, Week, Day, Page # pylint: disable=wrong-import-position
from dynamo.data import DynamoClient # pylint: disable=wrong-import-position


@pytest.fixture
def visitor():
  return Visitor( '0.0.0.0' )

@pytest.fixture
def visit():
  return Visit(
    '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )

@pytest.fixture
def session():
  return Session( '2020-01-01T00:00:00.000Z', '0.0.0.0', 60.0, 60.0 )

@pytest.fixture
def visits():
  return[
    Visit(
      '2020-01-01T00:00:00.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '2020-01-01T00:01:00.000Z', '0.0.0.0', '0', 'Blog', '/blog',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    )
  ]

@pytest.fixture
def year_visits():
  return [
    Visit(
      '2020-01-01T00:00:00.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '2020-01-01T00:00:01.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '2020-01-01T00:00:00.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    )
]

@pytest.fixture
def month_visits():
  return [
    Visit(
      '2020-01-01T00:00:00.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '2020-01-03T00:00:01.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '2020-01-25T00:00:00.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    ),
    Visit(
      '2020-01-30T00:00:00.000Z', '0.0.0.2', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    )
  ]

@pytest.fixture
def week_visits():
  return [
    Visit(
      '2020-01-03T00:00:00.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '2020-01-03T00:00:01.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '2020-01-04T00:00:00.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    ),
  ]

@pytest.fixture
def day_visits():
  return [
    Visit(
      '2020-01-03T00:00:00.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '2020-01-03T00:00:01.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '2020-01-03T00:00:00.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    ),
  ]

@pytest.fixture
def browser():
  return Browser(
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) ' + \
      'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 ' + \
      'Mobile/15E148 Safari/604.1',
    '0.0.0.0', 100, 200, '2020-01-01T00:00:00.000Z'
  )

@pytest.fixture
def browsers():
  return[
    Browser(
      'Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) ' + \
        'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 ' + \
        'Mobile/15E148 Safari/604.1',
      '0.0.0.0', 100, 200, '2020-01-01T00:00:00.000Z',
      dateAdded = '2020-01-01T00:00:00.000Z'
    ),
    Browser(
      'Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 ' + \
        '(KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36',
      '0.0.0.0', 100, 200, '2020-01-01T00:01:00.000Z',
      dateAdded = '2020-01-01T00:00:00.000Z'
    )
  ]

@pytest.fixture
def year( year_visits ):
  toPages = [ visit.nextSlug for visit in year_visits ]
  fromPages = [ visit.prevSlug for visit in year_visits ]
  return Year(
    year_visits[0].slug,
    year_visits[0].title,
    year_visits[0].date.strftime( '%Y' ),
    len( { visit.ip for visit in year_visits } ),
    np.mean( [
        visit.timeOnPage for visit in year_visits
        if visit.timeOnPage is not None
    ] ),
    toPages.count( None ) / len( toPages ),
    {
      (
        'www' if page is None else page
      ): fromPages.count( page ) / len( fromPages )
      for page in list( set( fromPages ) )
    },
    {
      (
        'www' if page is None else page
      ): toPages.count( page ) / len( toPages )
      for page in list( set( toPages ) )
    }
  )

@pytest.fixture
def month( month_visits ):
  toPages = [ visit.nextSlug for visit in month_visits ]
  fromPages = [ visit.prevSlug for visit in month_visits ]
  return Month(
    month_visits[0].slug,
    month_visits[0].title,
    month_visits[0].date.strftime( '%Y-%m' ),
    len( { visit.ip for visit in month_visits } ),
    np.mean( [
        visit.timeOnPage for visit in month_visits
        if visit.timeOnPage is not None
    ] ),
    toPages.count( None ) / len( toPages ),
    {
      (
        'www' if page is None else page
      ): fromPages.count( page ) / len( fromPages )
      for page in list( set( fromPages ) )
    },
    {
      (
        'www' if page is None else page
      ): toPages.count( page ) / len( toPages )
      for page in list( set( toPages ) )
    }
  )

@pytest.fixture
def week( week_visits ):
  toPages = [ visit.nextSlug for visit in week_visits ]
  fromPages = [ visit.prevSlug for visit in week_visits ]
  return Week(
    week_visits[0].slug,
    week_visits[0].title,
    week_visits[0].date.strftime( '%Y-%U' ),
    len( { visit.ip for visit in week_visits } ),
    np.mean( [
        visit.timeOnPage for visit in week_visits
        if visit.timeOnPage is not None
    ] ),
    toPages.count( None ) / len( toPages ),
    {
      (
        'www' if page is None else page
      ): fromPages.count( page ) / len( fromPages )
      for page in list( set( fromPages ) )
    },
    {
      (
        'www' if page is None else page
      ): toPages.count( page ) / len( toPages )
      for page in list( set( toPages ) )
    }
  )

@pytest.fixture
def day( day_visits ):
  toPages = [ visit.nextSlug for visit in day_visits ]
  fromPages = [ visit.prevSlug for visit in day_visits ]
  return Day(
    day_visits[0].slug,
    day_visits[0].title,
    day_visits[0].date.strftime( '%Y-%m-%d' ),
    len( { visit.ip for visit in day_visits } ),
    np.mean( [
        visit.timeOnPage for visit in day_visits
        if visit.timeOnPage is not None
    ] ),
    toPages.count( None ) / len( toPages ),
    {
      (
        'www' if page is None else page
      ): fromPages.count( page ) / len( fromPages )
      for page in list( set( fromPages ) )
    },
    {
      (
        'www' if page is None else page
      ): toPages.count( page ) / len( toPages )
      for page in list( set( toPages ) )
    }
  )

@pytest.fixture
def page( year_visits ):
  toPages = [ visit.nextSlug for visit in year_visits ]
  fromPages = [ visit.prevSlug for visit in year_visits ]
  return Page(
    year_visits[0].slug,
    year_visits[0].title,
    len( { visit.ip for visit in year_visits } ),
    np.mean( [
        visit.timeOnPage for visit in year_visits
        if visit.timeOnPage is not None
    ] ),
    toPages.count( None ) / len( toPages ),
    {
      (
        'www' if page is None else page
      ): fromPages.count( page ) / len( fromPages )
      for page in list( set( fromPages ) )
    },
    {
      (
        'www' if page is None else page
      ): toPages.count( page ) / len( toPages )
      for page in list( set( toPages ) )
    }
  )

@pytest.fixture
def table_init( dynamo_client, table_name ):
  dynamo_client.create_table(
    TableName=table_name,
    AttributeDefinitions=[
      { 'AttributeName': 'PK', 'AttributeType': 'S' },
      { 'AttributeName': 'SK', 'AttributeType': 'S' },
      { 'AttributeName': 'GSI1PK', 'AttributeType': 'S' },
      { 'AttributeName': 'GSI1SK', 'AttributeType': 'S' },
      { 'AttributeName': 'GSI2PK', 'AttributeType': 'S' },
      { 'AttributeName': 'GSI2SK', 'AttributeType': 'S' }
    ],
    KeySchema=[
      { 'AttributeName': 'PK', 'KeyType': 'HASH' },
      { 'AttributeName': 'SK', 'KeyType': 'RANGE' },
    ],
    GlobalSecondaryIndexes=[
      {
        'IndexName': 'GSI1',
        'KeySchema': [
          { 'AttributeName': 'GSI1PK', 'KeyType': 'HASH' },
          { 'AttributeName': 'GSI1SK', 'KeyType': 'RANGE' }
        ],
        'Projection': {
          'ProjectionType': 'ALL',
          'NonKeyAttributes': [ 'PK' ]
        },
        'ProvisionedThroughput': {
          'ReadCapacityUnits': 5,
          'WriteCapacityUnits': 5
        }
      },
      {
        'IndexName': 'GSI2',
        'KeySchema': [
          { 'AttributeName': 'GSI2PK', 'KeyType': 'HASH' },
          { 'AttributeName': 'GSI2SK', 'KeyType': 'RANGE' }
        ],
        'Projection': {
          'ProjectionType': 'ALL',
          'NonKeyAttributes': [ 'PK' ]
        },
        'ProvisionedThroughput': {
          'ReadCapacityUnits': 5,
          'WriteCapacityUnits': 5
        }
      },
    ]
  )
  yield

@pytest.fixture
def table_del( dynamo_client, table_name ):
  dynamo_client.delete_table( table_name )

def test_addVisit( dynamo_client, table_init, table_name, visit ):
  result = DynamoClient( table_name ).addVisit( visit )
  assert 'visit' in result.keys()
  assert result['visit'] == visit

def test_duplicate_addVisit( dynamo_client, table_init, table_name, visit ):
  client = DynamoClient( table_name )
  client.addVisit( visit )
  result = client.addVisit( visit )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor\'s page visit is already in table ' \
    + f'{ visit }'

def test_parameter_addVisit( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addVisit( {} )
  assert str( e.value ) == 'Must pass a Visit object'

def test_removeVisit( dynamo_client, table_init, table_name, visit ):
  client = DynamoClient( table_name )
  client.addVisit( visit )
  result = client.removeVisit( visit )
  assert 'visit' in result.keys()
  assert result['visit'] == visit

def test_none_removeVisit(
  dynamo_client, table_init, table_name, visit
):
  result = DynamoClient( table_name ).removeVisit( visit )
  assert 'error' in result.keys()
  assert result['error'] == f'Visit not in table { visit }'

def test_parameter_removeVisit( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).removeVisit( {} )
  assert str( e.value ) == 'Must pass a Visit object'

def test_addVisits( dynamo_client, table_init, table_name, visits ):
  result = DynamoClient( table_name ).addVisits( visits )
  assert 'visits' in result.keys()
  assert result['visits'] == visits

def test_parameter_addVisits( dynamo_client, table_init, table_name, visits ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addVisits( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_elements_addVisits(
  dynamo_client, table_init, table_name, visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addVisits( visits + [5] )
  assert str( e.value ) == 'Must pass Visit objects'

def test_addSession( dynamo_client, table_init, table_name, session ):
  result = DynamoClient( table_name ).addSession( session )
  assert 'session' in result.keys()
  assert result['session'] == session

def test_duplicate_addSession(
  dynamo_client, table_init, table_name, session
):
  client = DynamoClient( table_name )
  client.addSession( session )
  result = client.addSession( session )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor\'s session is already in table ' \
    + f'{ session }'

def test_parameter_addSession( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addSession( {} )
  assert str( e.value ) == 'Must pass a Session object'

def test_removeSession( dynamo_client, table_init, table_name, session ):
  client = DynamoClient( table_name )
  client.addSession( session )
  result = client.removeSession( session )
  assert 'session' in result.keys()
  assert result['session'] == session

def test_none_removeSession(
  dynamo_client, table_init, table_name, session
):
  result = DynamoClient( table_name ).removeSession( session )
  assert 'error' in result.keys()
  assert result['error'] == f'Session not in table { session }'

def test_parameter_removeSession( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).removeSession( {} )
  assert str( e.value ) == 'Must pass a Session object'

def test_addBrowser( dynamo_client, table_init, table_name, browser ):
  result = DynamoClient( table_name ).addBrowser( browser )
  assert 'browser' in result.keys()
  assert result['browser'] == browser

def test_duplicate_addBrowser(
  dynamo_client, table_init, table_name, browser
):
  client = DynamoClient( table_name )
  client.addBrowser( browser )
  result = client.addBrowser( browser )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor\'s browser is already in table ' \
    + f'{ browser }'

def test_parameter_addBrowser( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addBrowser( {} )
  assert str( e.value ) == 'Must pass a Browser object'

def test_removeBrowser( dynamo_client, table_init, table_name, browser ):
  client = DynamoClient( table_name )
  client.addBrowser( browser )
  result = client.removeBrowser( browser )
  assert 'browser' in result.keys()
  assert result['browser'] == browser

def test_none_removeBrowser(
  dynamo_client, table_init, table_name, browser
):
  result = DynamoClient( table_name ).removeBrowser( browser )
  assert 'error' in result.keys()
  assert result['error'] == f'Browser not in table { browser }'

def test_parameter_removeBrowser( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).removeBrowser( {} )
  assert str( e.value ) == 'Must pass a Browser object'

def test_addBrowsers( dynamo_client, table_init, table_name, browsers ):
  result = DynamoClient( table_name ).addBrowsers( browsers )
  assert 'browsers' in result.keys()
  assert result['browsers'] == browsers

def test_parameter_addBrowsers(
  dynamo_client, table_init, table_name,
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addBrowsers( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_elements_addBrowsers(
  dynamo_client, table_init, table_name, browsers
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addBrowsers( ( browsers ) + [5] )
  assert str( e.value ) == 'Must pass Browser objects'

def test_addNewSession(
  dynamo_client, table_init, table_name, visitor, browsers, visits
):
  client = DynamoClient( table_name )
  client.addVisitor( visitor )
  result = client.addNewSession(
    visitor, browsers, visits
  )
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor
  assert 'browsers' in result.keys()
  assert result['browsers'] == browsers
  assert 'visits' in result.keys()
  assert result['visits'] == visits
  assert 'session' in result.keys()
  assert result['session'].ip == visitor.ip
  assert result['session'].sessionStart == visits[0].date

def test_visitor_addNewSession(
  dynamo_client, table_init, table_name, visitor, browsers, visits
):
  client = DynamoClient( table_name )
  result = client.addNewSession(
    visitor, browsers, visits
  )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor not in table'

def test_none_getSessionDetails(
  dynamo_client, table_init, table_name, session
):
  client = DynamoClient( table_name )
  result = client.getSessionDetails( session )
  assert 'error' in result.keys()
  assert result['error'] == 'Session not in table'

def test_getSessionDetails(
  dynamo_client, table_init, table_name, visitor, browsers, visits, session
):
  client = DynamoClient( table_name )
  client.addVisitor( visitor )
  client.addNewSession(
    visitor, browsers, visits
  )
  result = client.getSessionDetails( session )
  assert 'visits' in result.keys()
  assert all( [
    dict( result['visits'][index] ) == dict(visits[index])
    for index in range( len( visits ) )
  ] )
  assert 'session' in result.keys()
  assert result['session'].ip == visitor.ip
  assert result['session'].sessionStart == visits[0].date

def test_table_getSessionDetails(
  dynamo_client, table_name, visitor, browsers, visits, session
):
  result = DynamoClient( table_name ).getSessionDetails( session )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not get session from table'

def test_updateSession(
  dynamo_client, table_init, table_name, visitor, browsers, visits, session,
):
  client = DynamoClient( table_name )
  client.addNewSession( visitor, browsers, visits )
  client.addSession( session )
  result = client.updateSession( session, visits )
  assert 'visits' in result.keys()
  assert all( [
    dict( result['visits'][index] ) == dict(visits[index])
    for index in range( len( visits ) )
  ] )
  assert 'session' in result.keys()
  assert dict( result['session'] ) == dict( session )

def test_parameter_session_type_updateSession(
  dynamo_client, table_name, browsers, visits, session
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).updateSession( {}, visits )
  assert str( e.value ) == 'Must pass a Session object'

def test_parameter_visit_type_updateSession(
  dynamo_client, table_name, browsers, visits, session
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).updateSession( session, {} )
  assert str( e.value ) == 'Must pass a list of Visit objects'

def test_parameter_visits_updateSession(
  dynamo_client, table_name, browsers, visits, session
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).updateSession( session, visits + [5] )
  assert str( e.value ) == 'List of visits must be of Visit type'

def test_table_updateSession(
  dynamo_client, table_name, browsers, visits, session
):
  result = DynamoClient( table_name ).updateSession( session, visits )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not update session in table'

def test_none_updateSession(
  dynamo_client, table_init, table_name, browsers, visits, session
):
  client = DynamoClient( table_name )
  client.addBrowsers( browsers )
  client.addVisits( visits )
  result = client.updateSession( session, visits )
  assert 'error' in result.keys()
  assert result['error'] == f'Session not in table { session }'

def test_addYear( dynamo_client, table_init, table_name, year_visits, year ):
  result = DynamoClient( table_name ).addYear( year_visits )
  assert 'year' in result.keys()
  assert dict( result['year'] ) == dict( year )

def test_table_addYear( dynamo_client, table_name, year_visits ):
  result = DynamoClient( table_name ).addYear( year_visits )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new year to table'

def test_parameter_list_addYear( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addYear( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_slug_addYear(
  dynamo_client, table_init, table_name, year_visits, visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addYear( year_visits + visits )
  assert str( e.value ) == 'List of visits must have the same slug'

def test_parameter_title_addYear(
  dynamo_client, table_init, table_name, year_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addYear( year_visits + [
      Visit(
        '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Resume', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must have the same title'

def test_parameter_year_addYear(
  dynamo_client, table_init, table_name, year_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addYear( year_visits + [
      Visit(
        '2021-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must be from the same year'

def test_addMonth( dynamo_client, table_init, table_name, month_visits, month ):
  result = DynamoClient( table_name ).addMonth( month_visits )
  assert 'month' in result.keys()
  assert dict( result['month'] ) == dict( month )

def test_table_addMonth( dynamo_client, table_name, month_visits ):
  result = DynamoClient( table_name ).addMonth( month_visits )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new month to table'

def test_parameter_list_addMonth( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addMonth( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_slug_addMonth(
  dynamo_client, table_init, table_name, month_visits, visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addMonth( month_visits + visits )
  assert str( e.value ) == 'List of visits must have the same slug'

def test_parameter_title_addMonth(
  dynamo_client, table_init, table_name, month_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addMonth( month_visits + [
      Visit(
        '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Resume', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must have the same title'

def test_parameter_year_addMonth(
  dynamo_client, table_init, table_name, month_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addMonth( month_visits + [
      Visit(
        '2021-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must be from the same year and month'

def test_addWeek( dynamo_client, table_init, table_name, week_visits, week ):
  result = DynamoClient( table_name ).addWeek( week_visits )
  assert 'week' in result.keys()
  assert dict( result['week'] ) == dict( week )

def test_table_addWeek( dynamo_client, table_name, week_visits ):
  result = DynamoClient( table_name ).addWeek( week_visits )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new week to table'

def test_parameter_list_addWeek( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addWeek( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_slug_addWeek(
  dynamo_client, table_init, table_name, week_visits, visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addWeek( week_visits + visits )
  assert str( e.value ) == 'List of visits must have the same slug'

def test_parameter_title_addWeek(
  dynamo_client, table_init, table_name, week_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addWeek( week_visits + [
      Visit(
        '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Resume', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must have the same title'

def test_parameter_year_addWeek(
  dynamo_client, table_init, table_name, week_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addWeek( week_visits + [
      Visit(
        '2021-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must be from the same year and week'

def test_addDay( dynamo_client, table_init, table_name, day_visits, day ):
  result = DynamoClient( table_name ).addDay( day_visits )
  assert 'day' in result.keys()
  assert dict( result['day'] ) == dict( day )

def test_table_addDay( dynamo_client, table_name, day_visits ):
  result = DynamoClient( table_name ).addDay( day_visits )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new day to table'

def test_parameter_list_addDay( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addDay( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_slug_addDay(
  dynamo_client, table_init, table_name, day_visits, visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addDay( day_visits + visits )
  assert str( e.value ) == 'List of visits must have the same slug'

def test_parameter_title_addDay(
  dynamo_client, table_init, table_name, day_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addDay( day_visits + [
      Visit(
        '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Resume', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must have the same title'

def test_parameter_year_addDay(
  dynamo_client, table_init, table_name, day_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addDay( day_visits + [
      Visit(
        '2021-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must be from the same year, ' + \
    'month, and day'

def test_addPage( dynamo_client, table_init, table_name, year_visits, page ):
  result = DynamoClient( table_name ).addPage( year_visits )
  assert 'page' in result.keys()
  assert dict( result['page'] ) == dict( page )

def test_table_addPage( dynamo_client, table_name, year_visits ):
  result = DynamoClient( table_name ).addPage( year_visits )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new page to table'

def test_parameter_list_addPage( dynamo_client, table_init, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addPage( {} )
  assert str( e.value ) == 'Must pass a list'

def test_parameter_slug_addPage(
  dynamo_client, table_init, table_name, year_visits, visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addPage( year_visits + visits )
  assert str( e.value ) == 'List of visits must have the same slug'

def test_parameter_title_addPage(
  dynamo_client, table_init, table_name, year_visits
):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addPage( year_visits + [
      Visit(
        '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Resume', '/',
        '2020-12-23T20:32:26.000Z'
      )
    ] )
  assert str( e.value ) == 'List of visits must have the same title'
