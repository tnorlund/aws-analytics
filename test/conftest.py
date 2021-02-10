# pylint: disable=redefined-outer-name, unused-argument
import os
import boto3
import pytest

from moto import mock_dynamodb2, mock_s3
from dynamo.entities import Visitor, Browser, Visit, Session, Page

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
def s3_client( aws_credentials ):
  with mock_s3():
    conn = boto3.client( 's3', region_name = 'us-east-1' )
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
def bucket_name():
  '''The name of the mocked S3 bucket.'''
  return 'test-bucket'

@pytest.fixture
def s3_init( s3_client, bucket_name ):
  '''Mocked the creation of a S3 bucket.'''
  s3_client.create_bucket( Bucket = bucket_name )
  yield

@pytest.fixture
def ip():
  '''A properly formatted IP address.'''
  return '0.0.0.0'

@pytest.fixture
def visitor():
  return Visitor( '171a0329-f8b2-499c-867d-1942384ddd5f' )

@pytest.fixture
def visit():
  return Visit(
    '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-12-23T20:32:26.000Z', '0', 'Tyler Norlund', '/',
    '2020-12-23T20:32:26.000Z'
  )

@pytest.fixture
def visits():
  return[
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-01-01T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-01-01T00:01:00.000Z', '0', 'Blog', '/blog',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    )
  ]

@pytest.fixture
def browsers():
  return[
    Browser(
      '171a0329-f8b2-499c-867d-1942384ddd5f', 
      'Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) ' + \
        'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 ' + \
        'Mobile/15E148 Safari/604.1',
      100, 200, '2020-01-01T00:00:00.000Z',
      dateAdded = '2020-01-01T00:00:00.000Z'
    ),
    Browser(
      '171a0329-f8b2-499c-867d-1942384ddd5f', 
      'Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 ' + \
        '(KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36',
      100, 200, '2020-01-01T00:01:00.000Z',
      dateAdded = '2020-01-01T00:00:00.000Z'
    )
  ]

@pytest.fixture
def year_visits():
  return [
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-01-01T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-01T00:00:01.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-01T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    )
]

@pytest.fixture
def month_visits():
  return [
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-01-01T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-03T00:00:01.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-25T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5s', '2020-01-30T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-01T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    )
  ]

@pytest.fixture
def week_visits():
  return [
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-01-03T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-03T00:00:01.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-04T00:00:00.000Z', '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    ),
  ]

@pytest.fixture
def day_visits():
  return [
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5f', '2020-01-03T00:00:00.000Z',  '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '60', None, None, 'Blog', '/blog'
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-03T00:00:01.000Z',  '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', None, 'Tyler Norlund', '/', None, None
    ),
    Visit(
      '171a0329-f8b2-499c-867d-1942384ddd5a', '2020-01-03T00:00:00.000Z',  '0', 'Tyler Norlund', '/',
      '2020-01-03T00:00:00.000Z', '120', None, None, 'Resume', '/resume'
    ),
  ]

@pytest.fixture
def page():
  return Page(
    '/',
    'Tyler Norlund',
    2,
    90,
    0.3333333333333333,
    { '/': 0.3333333333333333, 'www': 0.6666666666666666 },
    {
      '/blog': 0.3333333333333333,
      '/resume': 0.3333333333333333,
      'www': 0.3333333333333333
    }
  )

@pytest.fixture
def session():
  '''A proper Session object.'''
  return Session( '2020-01-01T00:00:00.000Z', '171a0329-f8b2-499c-867d-1942384ddd5f', 60.0, 60.0 )

@pytest.fixture
def year_session():
  '''A proper Session object.'''
  return Session( '2020-01-01T00:00:00.000Z', '171a0329-f8b2-499c-867d-1942384ddd5f', 60.0, 60.0 )

@pytest.fixture
def samsung_G950U_app():
  '''The user agent of a Samsung phone'''
  return 'Mozilla/5.0 (Linux; Android 9; SM-G950U) AppleWebKit/537.36 ' + \
    '(KHTML, like Gecko) Chrome/87.0.4280.101 Mobile Safari/537.36'

@pytest.fixture
def samsung_G981U1_app():
  return 'Mozilla/5.0 (Linux; Android 10; SAMSUNG SM-G981U1) ' + \
    'AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/13.0 ' + \
    'Chrome/83.0.4103.106 Mobile Safari/537.36'

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
