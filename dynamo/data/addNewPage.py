import os
import sys
import pandas as  pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Page, Day, Week # pylint: disable=wrong-import-position
dynamo = boto3.client( 'dynamodb' )

def addNewPage( visits ):
  '''Adds the page, and its days/weeks/months/years to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The specific page's visits that are processed to the page, days, weeks,
    months, and years.

  Returns
  -------
  results : dict
    The result of adding the new page, days, weeks, months, and years. This
    could be either the error that occurs or the new page, days, weeks, months,
    and years.
  '''
  # TODO
  # [ ] Add months to table
  # [ ] Add years to table
  # [ ] Remove Pandas and use listcomprehension
  # EXAMPLE
  #   [
  #     [
  #       visit for visit in data['visits']
  #       if visit.date.strftime( '%Y-%U' ) == week
  #     ]
  #     for week in {
  #       visit.date.strftime( '%Y-%U' ) for visit in data['visits']
  #     }
  #   ]
  # Create a DF to easily process the dates
  visit_df = pd.DataFrame( { 'datetime': [ visit.date for visit in visits ] } )
  visit_df['day'] = visit_df['datetime'].dt.date
  visit_df['week'] = visit_df['datetime'].dt.isocalendar().week
  visit_df['month'] = visit_df['datetime'].dt.month
  visit_df['year'] = visit_df['datetime'].dt.year
  # Create a dictionary that has the days as keys, and the indexes of the
  # occurred day as its values.
  day_dict = {
    date.strftime( '%Y-%m-%d' ): visit_df.loc[
      visit_df['day'] == date
    ].index.tolist()
    for date in visit_df['day'].unique()
  }
  # Add each day's visits
  days = []
  for day_visits in [
    [ visits[index] for index in day_dict[date] ] for date in day_dict
  ]:
    day_result = addDay( day_visits )
    if 'error' in day_result.keys():
      return { 'error': day_result['error'] }
    days.append( day_result['day'] )
  # Add each week's visits
  week_df = visit_df.groupby( ['year'] )['week'].apply(
    lambda x: list( np.unique( x ) )
  )
  week_dict = {}
  for year in week_df.index:
    week_dict.update( {
        f'{year}-{week}':visit_df.loc[
            ( visit_df['year'] == year ) &
            ( visit_df['week'] == week )
        ].index.tolist()
        for week in week_df[year]
    } )
  weeks = []
  for week_visits in [
    [ visits[index] for index in week_dict[week] ] for week in week_dict
  ]:
    week_result = addWeek( week_visits )
    if 'error' in week_result.keys():
      return { 'error': week_result['error'] }
    days.append( week_result['week'] )
  # Add the page to the table
  page_result = addPage( visits )
  if 'error' in page_result.keys():
    return { 'error': page_result['error'] }
  return { 'page': page_result['page'], 'days': days, 'weeks': weeks }

def addWeek( visits ):
  '''Adds a week item to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The list of visits from a specific week for the specific page.

  Returns
  -------
  result : dict
    The result of adding the week to the table. This could be the error that
    occurs or the week object added to the table.
  '''
  # Find all the pages visited before and after this one.
  toPages = [ visit.nextSlug for visit in visits ]
  fromPages = [ visit.prevSlug for visit in visits ]
  try:
    # Create the week object
    week = Week(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y-%U' ),
      len( { visit.ip for visit in visits } ),
      np.mean( [
          visit.timeOnPage for visit in visits
          if visit.timeOnPage is not None
      ] ),
      toPages.count( None ) / len( toPages ),
      {
        (
          'www' if page is None else page
        ): fromPages.count( page ) / len( fromPages )
        for page in list( set( fromPages ) )
      },
      {
        (
          'www' if page is None else page
        ): toPages.count( page ) / len( toPages )
        for page in list( set( toPages ) )
      }
    )
    # Add the week to the table
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = week.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    # Return the week added to the table
    return { 'week': week }
  except ClientError as e:
    print( f'ERROR addWeek: { e }')
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return {
        'error': f'Week is already in table { week }'
      }
    return { 'error': 'Could not add new week to table' }

def addDay( visits ):
  '''Adds a day item to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The list of visits from a specific day for the specific page.

  Returns
  -------
  result : dict
    The result of adding the day to the table. This could be the error that
    occurs or the day object added to the table.
  '''
  # Find all the pages visited before and after this one.
  toPages = [ visit.nextSlug for visit in visits ]
  fromPages = [ visit.prevSlug for visit in visits ]
  try:
    # Create the day object.
    day = Day(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y-%m-%d' ),
      len( { visit.ip for visit in visits } ),
      np.mean( [
          visit.timeOnPage for visit in visits
          if visit.timeOnPage is not None
      ] ),
      toPages.count( None ) / len( toPages ),
      {
        (
          'www' if page is None else page
        ): fromPages.count( page ) / len( fromPages )
        for page in list( set( fromPages ) )
      },
      {
        (
          'www' if page is None else page
        ): toPages.count( page ) / len( toPages )
        for page in list( set( toPages ) )
      }
    )
    # Add the day to the table
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = day.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    # Return the day added to the table
    return { 'day': day }
  except ClientError as e:
    print( f'ERROR addDay: { e }')
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return {
        'error': f'Day is already in table { day }'
      }
    return { 'error': 'Could not add new day to table' }

def addPage( visits ):
  '''Adds the page item to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The list of all visits for the specific page.

  Returns
  -------
  result : dict
    The result of adding the page to the table. This could be the error that
    occurs or the page object added to the table.
  '''
  # Find all the pages visited before and after this one.
  toPages = [ visit.nextSlug for visit in visits ]
  fromPages = [ visit.prevSlug for visit in visits ]
  try:
    # Create the page object.
    page = Page(
      visits[0].slug,
      visits[0].title,
      len( { visit.ip for visit in visits } ),
      np.mean( [
          visit.timeOnPage for visit in visits
          if visit.timeOnPage is not None
      ] ),
      toPages.count( None ) / len( toPages ),
      {
        (
          'www' if page is None else page
        ): fromPages.count( page ) / len( fromPages )
        for page in list( set( fromPages ) )
      },
      {
        (
          'www' if page is None else page
        ): toPages.count( page ) / len( toPages )
        for page in list( set( toPages ) )
      }
    )
    # Add the page to the table
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = page.toItem(),
      ConditionExpression = 'attribute_not_exists(PK)'
    )
    # Return the page added to the table
    return { 'page': page }
  except ClientError as e:
    print( f'ERROR addPage: { e }')
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
      return {
        'error': f'Page is already in table { page }'
      }
    return { 'error': 'Could not add new page to table' }
