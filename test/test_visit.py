import datetime
import pytest
from dynamo.entities import Visit, itemToVisit # pylint: disable=wrong-import-position

visitor_id = '171a0329-f8b2-499c-867d-1942384ddd5f'

def test_default_init():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.id == visitor_id
  assert visit.date == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.user == 0
  assert visit.title == 'Tyler Norlund'
  assert visit.slug == '/'
  assert visit.sessionStart == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.prevTitle is None
  assert visit.prevSlug is None
  assert visit.nextTitle is None
  assert visit.nextSlug is None
  assert visit.timeOnPage is None

def test_prev_init():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z', prevTitle = 'Tyler Norlund', prevSlug = '/'
  )
  assert visit.id == visitor_id
  assert visit.date == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.user == 0
  assert visit.title == 'Tyler Norlund'
  assert visit.slug == '/'
  assert visit.sessionStart == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.prevTitle == 'Tyler Norlund'
  assert visit.prevSlug == '/'
  assert visit.nextTitle is None
  assert visit.nextSlug is None
  assert visit.timeOnPage is None

def test_next_init():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z', nextTitle = 'Tyler Norlund', nextSlug = '/'
  )
  assert visit.id == visitor_id
  assert visit.date == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.user == 0
  assert visit.title == 'Tyler Norlund'
  assert visit.slug == '/'
  assert visit.sessionStart == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.prevTitle is None
  assert visit.prevSlug is None
  assert visit.nextTitle == 'Tyler Norlund'
  assert visit.nextSlug == '/'
  assert visit.timeOnPage is None

def test_no_user_init():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', None, 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.id == visitor_id
  assert visit.date == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.user == 0
  assert visit.title == 'Tyler Norlund'
  assert visit.slug == '/'
  assert visit.sessionStart == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit.prevTitle is None
  assert visit.prevSlug is None
  assert visit.nextTitle is None
  assert visit.nextSlug is None
  assert visit.timeOnPage is None

def test_key():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.key() == {
    'PK': { 'S': f'VISITOR#{ visitor_id }' },
    'SK': { 'S': 'VISIT#2020-12-23T20:32:26.000Z#/' }
  }

def test_pk():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.pk() == { 'S': f'VISITOR#{ visitor_id }' }

def test_gsi1():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.gsi1() == {
    'GSI1PK': { 'S': 'PAGE#/' },
    'GSI1SK': { 'S': 'VISIT#2020-12-23T20:32:26.000Z' }
  }

def test_gsi1pk():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.gsi1pk() == { 'S': 'PAGE#/' }

def test_gsi2():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.gsi2() == {
    'GSI2PK': { 'S': f'SESSION#{ visitor_id }#2020-12-23T20:32:26.000Z'},
    'GSI2SK': { 'S': 'VISIT#2020-12-23T20:32:26.000Z'}
  }

def test_gsi2pk():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.gsi2pk() == {
    'S': f'SESSION#{ visitor_id }#2020-12-23T20:32:26.000Z'
  }

def test_toItem():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert visit.toItem() == {
    'PK': { 'S': f'VISITOR#{ visitor_id }' },
    'SK': { 'S': 'VISIT#2020-12-23T20:32:26.000Z#/' },
    'GSI1PK': { 'S': 'PAGE#/' },
    'GSI1SK': { 'S': 'VISIT#2020-12-23T20:32:26.000Z' },
    'GSI2PK': { 'S': f'SESSION#{ visitor_id }#2020-12-23T20:32:26.000Z' },
    'GSI2SK': { 'S': 'VISIT#2020-12-23T20:32:26.000Z' },
    'Type': { 'S': 'visit' },
    'User': { 'N': '0' },
    'Title': { 'S': 'Tyler Norlund' },
    'Slug': { 'S': '/' },
    'PreviousTitle': { 'NULL': True },
    'PreviousSlug': { 'NULL': True },
    'NextTitle': { 'NULL': True },
    'NextSlug': { 'NULL': True },
    'TimeOnPage': { 'NULL': True }
  }

def test_repr():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  assert repr( visit ) == f'{ visitor_id } - 2020-12-23T20:32:26.000Z'

def test_dict():
  visit = dict( Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  ) )
  assert visit['date'] == datetime.datetime( 2020, 12, 23, 20, 32, 26 )
  assert visit['id'] == visitor_id
  assert visit['user'] == 0
  assert visit['title'] == 'Tyler Norlund'
  assert visit['slug'] == '/'
  assert visit['prevTitle'] is None
  assert visit['prevSlug'] is None
  assert visit['nextTitle'] is None
  assert visit['nextSlug'] is None
  assert visit['timeOnPage'] is None

def test_itemToVisit():
  visit = Visit(
    visitor_id, '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )
  item = visit.toItem()
  newVisit = itemToVisit( item )
  assert newVisit.id == visit.id
  assert newVisit.date == visit.date
  assert newVisit.user == visit.user
  assert newVisit.title == visit.title
  assert newVisit.slug == visit.slug
  assert newVisit.sessionStart == visit.sessionStart
  assert newVisit.prevTitle == visit.prevTitle
  assert newVisit.prevSlug == visit.prevTitle
  assert newVisit.nextTitle == visit.nextTitle
  assert newVisit.nextSlug == visit.nextSlug
  assert newVisit.timeOnPage == visit.timeOnPage

def test_itemToVisit_exception():
  with pytest.raises( Exception ) as e:
    assert itemToVisit( {} )
  assert str( e.value ) == "Could not parse visit"
