import datetime
from dotenv import load_dotenv
import pytest
load_dotenv()
from dynamo.entities import Session, itemToSession # pylint: disable=wrong-import-position

def test_default_init():
  session = Session( '2020-01-01T00:00:00.000Z', '0.0.0.0', 0.1, 0.1 )
  assert session.sessionStart == datetime.datetime( 2020, 1, 1, 0, 0 )
  assert session.ip == '0.0.0.0'
  assert session.avgTime == 0.1
  assert session.totalTime == 0.1

def test_datetime_init():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert session.sessionStart == datetime.datetime( 2020, 1, 1, 0, 0 )
  assert session.ip == '0.0.0.0'
  assert session.avgTime == 0.1
  assert session.totalTime == 0.1

def test_key():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert session.key() == {
    'PK': { 'S': 'VISITOR#0.0.0.0' },
    'SK': { 'S': 'SESSION#2020-01-01T00:00:00.000Z' }
  }

def test_pk():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert session.pk() == { 'S': 'VISITOR#0.0.0.0' }

def test_gsi2():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert session.gsi2() == {
    'GSI2PK': { 'S': 'SESSION#0.0.0.0#2020-01-01T00:00:00.000Z' },
    'GSI2SK': { 'S': '#SESSION' }
  }

def test_gsi2pk():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert session.gsi2pk() == {
    'S': 'SESSION#0.0.0.0#2020-01-01T00:00:00.000Z'
  }

def test_toItem():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert session.toItem() == {
    'PK': { 'S': 'VISITOR#0.0.0.0' },
    'SK': { 'S': 'SESSION#2020-01-01T00:00:00.000Z' },
    'GSI2PK': { 'S': 'SESSION#0.0.0.0#2020-01-01T00:00:00.000Z' },
    'GSI2SK': { 'S': '#SESSION' },
    'Type': { 'S': 'session' },
    'AverageTime': { 'N': '0.1' },
    'TotalTime': { 'N': '0.1' }
  }

def test_repr():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  assert repr( session ) == '0.0.0.0 - 0.1'

def test_itemToSession():
  session = Session(
    datetime.datetime( 2020, 1, 1, 0, 0 ), '0.0.0.0', 0.1, 0.1
  )
  newSession = itemToSession( session.toItem() )
  assert newSession.sessionStart == session.sessionStart
  assert newSession.ip == session.ip
  assert newSession.avgTime == session.avgTime
  assert newSession.totalTime == session.totalTime

def test_itemToSession_exception():
  with pytest.raises( Exception ) as e:
    assert itemToSession( {} )
  assert str( e.value ) == "Could not parse session"
