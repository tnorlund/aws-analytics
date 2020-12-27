import pytest
from dynamo.data import DynamoClient

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_addVisitor( table_name, visitor ):
  client = DynamoClient( table_name )
  result = client.addVisitor( visitor )
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_duplicate_addVisitor( table_name, visitor ):
  client = DynamoClient( table_name )
  result = client.addVisitor( visitor )
  result = client.addVisitor( visitor )
  assert 'error' in result.keys()
  assert result['error'] == f'Visitor already in table { visitor }'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_parameter_addVisitor( table_name ):
  client = DynamoClient( table_name )
  with pytest.raises( ValueError ) as e:
    assert client.addVisitor( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_table_addVisitor( visitor ):
  client = DynamoClient( 'no name' )
  result = client.addVisitor( visitor )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not add new visitor to table'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_removeVisitor( table_name, visitor ):
  client = DynamoClient( table_name )
  client.addVisitor( visitor )
  result = client.removeVisitor( visitor )
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_none_removeVisitor( table_name, visitor ):
  client = DynamoClient( table_name )
  result = client.removeVisitor( visitor )
  assert 'error' in result.keys()
  assert result['error'] == f'Visitor not in table { visitor }'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_parameter_removeVisitor( table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).removeVisitor( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_incrementVisitorSessions( table_name, visitor ):
  client = DynamoClient( table_name )
  client.addVisitor( visitor )
  result = client.incrementVisitorSessions( visitor )
  visitor.numberSessions += 1
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_none_incrementVisitorSessions( table_name, visitor ):
  result = DynamoClient( table_name ).incrementVisitorSessions( visitor )
  visitor.numberSessions += 1
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor not in table'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_parameter_incrementVisitorSessions( table_name ):
  with pytest.raises( ValueError ) as e:
    assert  DynamoClient( table_name ).incrementVisitorSessions( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_addNewVisitor(
  table_name, visitor, browsers, visits, session, location
):
  client = DynamoClient( table_name )
  result = client.addNewVisitor(
    visitor, location, browsers, visits
  )
  assert 'visitor' in result.keys()
  assert result['visitor'] == visitor
  assert 'browsers' in result.keys()
  assert result['browsers'] == browsers
  assert 'location' in result.keys()
  assert result['location'] == location
  assert 'visits' in result.keys()
  assert result['visits'] == visits
  assert 'session' in result.keys()
  assert dict( result['session'] ) == dict( session )

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_duplicate_visitor_addNewVisitor(
  table_name, visitor, browsers, visits, location
):
  client = DynamoClient( table_name )
  result = client.addVisitor( visitor )
  result = client.addNewVisitor(
    visitor, location, browsers, visits
  )
  assert 'error' in result.keys()
  assert result['error'] == f'Visitor already in table { visitor }'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_duplicate_location_addNewVisitor(
  table_name, visitor, browsers, visits, location
):
  client = DynamoClient( table_name )
  result = client.addLocation( location )
  result = client.addNewVisitor(
    visitor, location, browsers, visits
  )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor\'s location is already in table ' + \
    f'{ location }'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_duplicate_session_addNewVisitor(
  table_name, visitor, browsers, visits, location, session
):
  client = DynamoClient( table_name )
  result = client.addSession( session )
  result = client.addNewVisitor(
    visitor, location, browsers, visits
  )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor\'s session is already in table ' + \
    f'{ session }'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_getVisitorDetails(
  table_name, visitor, browsers, visits, location
):
  client = DynamoClient( table_name )
  client.addNewVisitor(
    visitor, location, browsers, visits
  )
  result = client.getVisitorDetails( visitor )
  assert 'visitor' in result.keys()
  assert dict( result['visitor'] ) == dict( visitor )
  assert 'browsers' in result.keys()
  assert all( [
    dict( result['browsers'][index] ) == dict( browsers[index] )
    for index in range( len( browsers ) )
  ] )
  assert 'location' in result.keys()
  assert dict( result['location'] ) == dict( location )
  assert 'visits' in result.keys()
  assert all( [
    dict( result['visits'][index] ) == dict(visits[index])
    for index in range( len( visits ) )
  ] )
  assert 'sessions' in result.keys()

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_parameter_getVisitorDetails( table_name ):
  with pytest.raises( ValueError ) as e:
    assert DynamoClient( table_name ).getVisitorDetails( {} )
  assert str( e.value ) == 'Must pass a Visitor object'

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_none_getVisitorDetails( table_name, visitor ):
  result = DynamoClient( table_name ).getVisitorDetails( visitor )
  assert 'error' in result.keys()
  assert result['error'] == 'Visitor not in table'

@pytest.mark.usefixtures( 'dynamo_client' )
def test_table_getVisitorDetails( table_name, visitor ):
  result = DynamoClient( table_name ).getVisitorDetails( visitor )
  assert 'error' in result.keys()
  assert result['error'] == 'Could not get visitor from table'
