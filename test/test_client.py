import pytest
from dynamo.entities import Session, Visit, Visitor, Location # pylint: disable=wrong-import-position
from dynamo.data import DynamoClient # pylint: disable=wrong-import-position

@pytest.fixture
def table_name():
  return "BlogDB-test"

@pytest.fixture
def visitor():
  return Visitor( '0.0.0.0' )

@pytest.fixture
def location():
  return Location( 
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'],
    {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    }, 'Charter Communications', False, False, False
  )

@pytest.fixture
def visit():
  return Visit(
    '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )

@pytest.fixture
def dynamo_test( dynamo_client, table_name ):
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

def test_none_getSessionDetails( dynamo_client, dynamo_test, table_name ):
  session = Session( '2020-01-01T00:00:00.000Z', '0.0.0.0', 0.1, 0.1 )
  client = DynamoClient( table_name )
  result = client.getSessionDetails( session )
  assert 'error' in result.keys()
  assert result['error'] == 'Session not in table'

def test_addVisitor( dynamo_client, dynamo_test, table_name, visitor ):
  client = DynamoClient( table_name )
  result = client.addVisitor( visitor )
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor

def test_duplicate_addVisitor(
  dynamo_client, dynamo_test, table_name, visitor
):
  client = DynamoClient( table_name )
  result = client.addVisitor( visitor )
  result = client.addVisitor( visitor )
  assert 'error' in result.keys()
  assert result['error'] == f'Visitor already in table { visitor }'

def test_parameter_addVisitor( dynamo_client, dynamo_test, table_name ):
  client = DynamoClient( table_name )
  with pytest.raises( ValueError ) as e:
    assert client.addVisitor( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

def test_table_addVisitor( dynamo_client, dynamo_test, visitor ):
  client = DynamoClient( 'no name' )
  result = client.addVisitor( visitor )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new visitor to table'

def test_removeVisitor( dynamo_client, dynamo_test, table_name, visitor ):
  client = DynamoClient( table_name )
  client.addVisitor( visitor )
  result = client.removeVisitor( visitor )
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor

def test_none_removeVisitor( dynamo_client, dynamo_test, table_name, visitor ):
  client = DynamoClient( table_name )
  result = client.removeVisitor( visitor )
  assert 'error' in result.keys()
  assert result['error'] == f'Visitor not in table { visitor }'

def test_parameter_removeVisitor( dynamo_client, dynamo_test, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).removeVisitor( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

def test_incrementVisitorSessions(
  dynamo_client, dynamo_test, table_name, visitor
):
  client = DynamoClient( table_name )
  client.addVisitor( visitor )
  result = client.incrementVisitorSessions( visitor )
  visitor.numberSessions += 1
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor

def test_none_incrementVisitorSessions(
  dynamo_client, dynamo_test, table_name, visitor
):
  result = DynamoClient( table_name ).incrementVisitorSessions( visitor )
  visitor.numberSessions += 1
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor not in table'

def test_parameter_incrementVisitorSessions(
  dynamo_client, dynamo_test, table_name
):
  with pytest.raises( ValueError ) as e:
    assert  DynamoClient( table_name ).incrementVisitorSessions( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

def test_addLocation( dynamo_client, dynamo_test, table_name, location ):
  result = DynamoClient( table_name ).addLocation( location )
  assert 'location' in result.keys()
  assert result['location'] == location

def test_duplicate_addLocation(
  dynamo_client, dynamo_test, table_name, location
):
  client = DynamoClient( table_name )
  client.addLocation( location )
  result = client.addLocation( location )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor\'s location is already in table ' \
    + f'{ location }'

def test_parameter_addLocation( dynamo_client, dynamo_test, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).addLocation( {} )
  assert str( e.value ) == 'Must pass a Location object'

def test_removeLocation( dynamo_client, dynamo_test, table_name, location ):
  client = DynamoClient( table_name )
  client.addLocation( location )
  result = client.removeLocation( location )
  assert 'location' in result.keys()
  assert result['location'] == location

def test_none_removeLocation(
  dynamo_client, dynamo_test, table_name, location
):
  result = DynamoClient( table_name ).removeLocation( location )
  assert 'error' in result.keys()
  assert result['error'] == f'Location not in table { location }'

def test_parameter_removeLocation( dynamo_client, dynamo_test, table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).removeLocation( {} )
  assert str( e.value ) == 'Must pass a Location object'

def test_addVisit( dynamo_client, dynamo_test, table_name, visit ):
  result = DynamoClient( table_name ).addVisit( visit )
  assert 'visit' in result.keys()
  assert result['visit'] == visit