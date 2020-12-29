import os
import sys
import numpy as np
import pandas as pd
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Visit, Browser # pylint: disable=wrong-import-position

def processDF( df, ip ):
  '''Cleans the given dataframe to only have the data of the given IP address.

  Parameters
  ----------
  df : pd.DataFrame
    The raw dataframe from the original '.parquet' file.
  ip : str
    The IP address of the requested visitor.

  Returns
  -------
  result : dict
    A dictionary that contains both the browsers and visits related to the
    given IP address.
  '''
  v_df = df[df['ip'] == ip].drop_duplicates().sort_values( by='id' ) \
    .reset_index()
  # Format the datetimes to be dates and then calculate the amount of time
  # between each request.
  v_df['seconds'] = pd.to_datetime(
    v_df['id'],
    format='%Y-%m-%dT%H:%M:%S.%fZ'
  ).diff(+1).dt.total_seconds()[1:].append(
    pd.Series( [ None ] )
  ).reset_index()[0]
  # Shift the slugs and title up and down in order to associate the
  # previous and next slugs and titles per each visit.
  v_df['prevSlug'] = v_df['slug'].shift( 1 )
  v_df['prevTitle'] = v_df['title'].shift( 1 )
  v_df['nextSlug'] = v_df['slug'].shift( -1 )
  v_df['nextTitle'] = v_df['title'].shift( -1 )
  # Get the indexes of the times when the visitor spent over 30 minutes on
  # a specific page.
  indexes = v_df.loc[ v_df['seconds'] > ( 60 * 30 ) ].index
  v_df.loc[ indexes, ['seconds', 'nextSlug', 'nextTitle']] = None
  v_df.loc[indexes + 1, ['prevSlug', 'prevTitle']] = None
  # Replace the NaN's with the None type for the entities
  v_df = v_df.replace( { np.nan: None } )
  # Parse the visits from the visitor's DF.
  visits = [
    Visit(
      row['id'], row['ip'], row['user'], row['title'], row['slug'],
      v_df.iloc[0]['id'], row['seconds'], row['prevTitle'],
      row['prevSlug'], row['nextTitle'], row['nextSlug']
    ) for index, row in v_df.iterrows()
  ]
  # Parse the browsers from the visitor's DF.
  browsers = [
    Browser( row['app'], row['ip'], row['width'], row['height'], row['id'] )
    for index, row in v_df.loc[
      v_df[ ['app', 'width', 'height'] ].drop_duplicates().dropna().index
    ].iterrows()
  ]
  return { 'visits': visits, 'browsers': browsers }
