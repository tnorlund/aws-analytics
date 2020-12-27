import pytest
from dynamo.entities import Visitor, itemToVisitor # pylint: disable=wrong-import-position

def test_default_init():
  visitor = Visitor( '0.0.0.0' )
  assert visitor.ip == '0.0.0.0'
  assert visitor.numberSessions == 0

def test_numberSessions_init():
  visitor = Visitor( '0.0.0.0', 1 )
  assert visitor.ip == '0.0.0.0'
  assert visitor.numberSessions == 1

def test_key():
  visitor = Visitor( '0.0.0.0', 1 )
  assert visitor.key() == {
    'PK': { 'S': 'VISITOR#0.0.0.0' },
    'SK': { 'S': '#VISITOR' }
  }

def test_pk():
  visitor = Visitor( '0.0.0.0', 1 )
  assert visitor.pk() == { 'S': 'VISITOR#0.0.0.0' }

def test_toItem():
  visitor = Visitor( '0.0.0.0', 1 )
  assert visitor.toItem() == {
    'PK': { 'S': 'VISITOR#0.0.0.0' },
    'SK': { 'S': '#VISITOR' },
    'Type': { 'S': 'visitor' },
    'NumberSessions': { 'N': '1' }
  }

def test_repr():
  visitor = Visitor( '0.0.0.0', 1 )
  assert repr( visitor ) == '0.0.0.0 - 1'

def test_itemToVisitor():
  visitor = Visitor( '0.0.0.0', 1 )
  newVisitor = itemToVisitor( visitor.toItem() )
  assert newVisitor.ip == visitor.ip
  assert newVisitor.numberSessions == visitor.numberSessions

def test_itemToVisit_exception():
  with pytest.raises( Exception ) as e:
    assert itemToVisitor( {} )
  assert str( e.value ) == "Could not parse visitor"
