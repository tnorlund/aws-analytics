import boto3

class S3Client():
  '''A class to represent an S3 client.
  '''
  def __init__( self, bucketName, regionName = 'us-east-1' ):
    '''Constructs the necessary attributes for the S3 client object.

    Parameters
    ----------
    bucketName : str
      The name of the S3 bucket.
    regionName : str
      The AWS region to connect to.
    '''
    self.client = boto3.client( 's3', region_name = regionName )
    self.bucketname = bucketName

  def getObject( self, key ):
    '''Gets an object from the S3 bucket.

    Parameters
    ----------
    key : str
      The path and file name of the object in the S3 bucket.
    '''
    return self.client.get_object(
      Bucket = self.bucketname,
      Key = key
    )

  def putObject( self, key, file ):
    '''Puts an object from the S3 bucket.

    Parameters
    ----------
    key : str
      The path and file name of the object in the S3 bucket.
    '''
    return self.client.put_object(
      Bucket = self.bucketname,
      Key = key,
      Body = file
    )

  def listParquet( self, prefix='/'):
    '''Lists the '.parquet' files in a bucket.

    Parameters
    ----------
    prefix : str, optional
      The prefix of the '.parquet' file requested. (default is '/')
    '''
    return self.client.list_objects_v2(
      Bucket = self.bucketname,
      Delimiter = 'parquet',
      Prefix = prefix
    )
