import datetime
import pytest
from dynamo.entities import Session, itemToSession # pylint: disable=wrong-import-position

visitor_id = '171a0329-f8b2-499c-867d-1942384ddd5f'

def test_default_init():
  session = Session( '2020-01-01T00:00:00.000Z', visitor_id, 0.1, 0.1 )
  assert session.sessionStart == datetime.datetime( 2020, 1, 1, 0, 0 )
  assert session.id == visitor_id
  assert session.avgTime == 0.1
  assert session.totalTime == 0.1

def test_datetime_init():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert session.sessionStart == datetime.datetime( 2020, 1, 1, 0, 0 )
  assert session.id == visitor_id
  assert session.avgTime == 0.1
  assert session.totalTime == 0.1

def test_key():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert session.key() == {
    'PK': { 'S': f'VISITOR#{ visitor_id }' },
    'SK': { 'S': 'SESSION#2020-01-01T00:00:00.000Z' }
  }

def test_pk():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert session.pk() == { 'S': f'VISITOR#{ visitor_id }' }

def test_gsi2():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert session.gsi2() == {
    'GSI2PK': { 'S': f'SESSION#{ visitor_id }#2020-01-01T00:00:00.000Z' },
    'GSI2SK': { 'S': '#SESSION' }
  }

def test_gsi2pk():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert session.gsi2pk() == {
    'S': f'SESSION#{ visitor_id }#2020-01-01T00:00:00.000Z'
  }

def test_toItem():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert session.toItem() == {
    'PK': { 'S': f'VISITOR#{ visitor_id }' },
    'SK': { 'S': 'SESSION#2020-01-01T00:00:00.000Z' },
    'GSI2PK': { 'S': f'SESSION#{ visitor_id }#2020-01-01T00:00:00.000Z' },
    'GSI2SK': { 'S': '#SESSION' },
    'Type': { 'S': 'session' },
    'AverageTime': { 'N': '0.1' },
    'TotalTime': { 'N': '0.1' }
  }

def test_repr():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), visitor_id, 0.1, 0.1
  )
  assert repr( session ) == f'{ visitor_id } - 0.1'

def test_itemToSession():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  newSession = itemToSession( session.toItem() )
  assert newSession.sessionStart == session.sessionStart
  assert newSession.id == session.id
  assert newSession.avgTime == session.avgTime
  assert newSession.totalTime == session.totalTime

def test_itemToSession_exception():
  with pytest.raises( Exception ) as e:
    assert itemToSession( {} )
  assert str( e.value ) == "Could not parse session"
