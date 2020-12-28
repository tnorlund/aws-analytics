import pytest
from dynamo.data import DynamoClient

@pytest.mark.usefixtures( 'dynamo_client', 'table_init' )
def test_updatePage(
  table_name, year_visits, month_visits, week_visits, day_visits
):
  client = DynamoClient( table_name )
  result = client.updatePage(
    month_visits + year_visits + month_visits + week_visits + day_visits
  )
  assert 'page' in result.keys() and 'days' in result.keys() and \
    'weeks' in result.keys() and 'months' in result.keys() and \
    'years' in result.keys()
