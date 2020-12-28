# pylint: disable=redefined-outer-name, unused-argument
import os
import boto3
import pytest

from moto import mock_dynamodb2
from dynamo.entities import Visitor, Browser, Visit, Session, Location

@pytest.fixture
def aws_credentials():
  '''Mocked AWS Credentials for moto.'''
  os.environ["AWS_ACCESS_KEY_ID"] = "testing"
  os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
  os.environ["AWS_SECURITY_TOKEN"] = "testing"
  os.environ["AWS_SESSION_TOKEN"] = "testing"
  os.environ['REGION_NAME'] = 'us-west-2'

@pytest.fixture
def dynamo_client( aws_credentials ):
  '''The mocked DynamoDB client'''
  with mock_dynamodb2():
    conn = boto3.client( "dynamodb", region_name="us-east-1" )
    yield conn

@pytest.fixture
def table_name():
  '''The name of the mocked DynamoDB table.'''
  return "BlogDB-test"

@pytest.fixture
def table_init( dynamo_client, table_name ):
  '''Mocked the creation of a DynamoDB table.'''
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
  '''Mocks the deletion of the DynamoDB table.'''
  dynamo_client.delete_table( table_name )

@pytest.fixture
def ip():
  '''A properly formatted IP address.'''
  return '0.0.0.0'

@pytest.fixture
def visitor():
  '''A proper Visit object.'''
  return Visitor( '0.0.0.0' )

@pytest.fixture
def location():
  '''A proper Location object.'''
  return Location(
    '0.0.0.0', 'US', 'California', 'Westlake Village', 34.141944,
    -118.819444, '91361', '-08:00', ['cpe-75-82-84-171.socal.res.rr.com'],
    {
      'asn': 20001,
      'name': 'Charter Communications (20001)',
      'route': '75.82.0.0/15',
      'domain': 'https://www.spectrum.com',
      'type': 'Cable/DSL/ISP'
    },
    'Charter Communications', False, False, False, '2020-01-01T00:00:00.000Z'
  )

@pytest.fixture
def browsers():
  '''A list of proper Browser objects.'''
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
def visit():
  '''A proper Visit object.'''
  return Visit(
    '2020-12-23T20:32:26.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )

@pytest.fixture
def visits():
  '''A list of proper Visit objects.'''
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
  '''A list of proper Visit objects that span a year.'''
  return [
    Visit(
      '2020-01-01T00:00:00.000Z', '0.0.0.0', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '2020-02-01T00:00:01.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '2020-03-01T00:00:00.000Z', '0.0.0.1', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    )
]

@pytest.fixture
def month_visits():
  '''A list of proper Visit objects that span a month.'''
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
  '''A list of proper Visit objects that span a week.'''
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
  '''A list of proper Visit objects that span a day.'''
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
def session():
  '''A proper Session object.'''
  return Session( '2020-01-01T00:00:00.000Z', '0.0.0.0', 60.0, 60.0 )


@pytest.fixture
def pixel_app():
  '''The user agent of a Pixel 4 XL.'''
  return 'Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 ' + \
    '(KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36'

@pytest.fixture
def mac_chrome_app():
  '''The user agent of a Mac using Chrome.'''
  return 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) ' + \
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'

@pytest.fixture
def mac_safari_app():
  '''The user agent of a Mac using Safari.'''
  return 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) ' + \
    'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15'

@pytest.fixture
def windows_chrome_app():
  '''The user agent of a Windows PC using Chrome.'''
  return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' + \
    '(KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'

@pytest.fixture
def iphone_safari_app():
  '''The user agent of an iPhone using Safari.'''
  return 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) ' + \
    'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Mobile/15E148 ' + \
    'Safari/604.1'

@pytest.fixture
def iphone_linkedin_app():
  '''The user agent of an iPhone using the browser in the LinkedIn app.'''
  return 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) ' + \
    'AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [LinkedInApp]'
