import os
import sys
import numpy as np
import boto3
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Page, Day, Week, Month, Year # pylint: disable=wrong-import-position

dynamo = boto3.client(
  'dynamodb', 
  region_name = os.environ.get( 'REGION_NAME' )
)

def updatePage( visits ):
  '''Adds the page, and its days/weeks/months/years to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The specific page's visits that are processed to the page, days, weeks,
    months, and years items.

  Returns
  -------
  results : dict
    The result of adding the new page, days, weeks, months, and years. This
    could be either the error that occurs or the new page, days, weeks, months,
    and years.
  '''
  # Add each day's visits
  days = []
  for day_visits in [
    [ visit for visit in visits if visit.date.strftime( '%Y-%m-%d' ) == day ]
    for day in { visit.date.strftime( '%Y-%m-%d' ) for visit in visits }
  ]:
    day_result = addDay( day_visits )
    if 'error' in day_result.keys():
      return { 'error': day_result['error'] }
    days.append( day_result['day'] )
  # Add each week's visits
  weeks = []
  for week_visits in [
    [ visit for visit in visits if visit.date.strftime( '%Y-%U' ) == week ]
    for week in { visit.date.strftime( '%Y-%U' ) for visit in visits }
  ]:
    week_result = addWeek( week_visits )
    if 'error' in week_result.keys():
      return { 'error': week_result['error'] }
    days.append( week_result['week'] )
  # Add each months's visits
  months = []
  for month_visits in [
    [ visit for visit in visits if visit.date.strftime( '%Y-%m' ) == month ]
    for month in { visit.date.strftime( '%Y-%m' ) for visit in visits }
  ]:
    month_result = addMonth( month_visits )
    if 'error' in month_result.keys():
      return { 'error': month_result['error'] }
    months.append( month_result['month'] )
  # Add each years's visits
  years = []
  for year_visits in [
    [ visit for visit in visits if visit.date.strftime( '%Y' ) == year ]
    for year in { visit.date.strftime( '%Y' ) for visit in visits }
  ]:
    year_result = addYear( year_visits )
    if 'error' in year_result.keys():
      return { 'error': year_result['error'] }
    years.append( year_result['year'] )
  # Add the page to the table
  page_result = addPage( visits )
  if 'error' in page_result.keys():
    return { 'error': page_result['error'] }
  return {
    'page': page_result['page'], 'days': days, 'weeks': weeks, 'months': months
  }

def addYear( visits ):
  '''Adds a year item to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The list of visits from a specific year for the specific page.

  Returns
  -------
  result : dict
    The result of adding the year to the table. This could be the error that
    occurs or the year object added to the table.
  '''
  # Find all the pages visited before and after this one.
  toPages = [ visit.nextSlug for visit in visits ]
  fromPages = [ visit.prevSlug for visit in visits ]
  try:
    # Create the week object
    year = Year(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y' ),
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
    # Add the year to the table
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = year.toItem()
    )
    # Return the year added to the table
    return { 'year': year }
  except ClientError as e:
    print( f'ERROR addYear: { e }' )
    return { 'error': 'Could not add new year to table' }

def addMonth( visits ):
  '''Adds a month item to the table.

  Parameters
  ----------
  visits : list[ Visit ]
    The list of visits from a specific month for the specific page.

  Returns
  -------
  result : dict
    The result of adding the month to the table. This could be the error that
    occurs or the month object added to the table.
  '''
  # Find all the pages visited before and after this one.
  toPages = [ visit.nextSlug for visit in visits ]
  fromPages = [ visit.prevSlug for visit in visits ]
  try:
    # Create the week object
    month = Month(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y-%m' ),
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
    # Add the month to the table
    dynamo.put_item(
      TableName = os.environ.get( 'TABLE_NAME' ),
      Item = month.toItem()
    )
    # Return the month added to the table
    return { 'month': month }
  except ClientError as e:
    print( f'ERROR addMonth: { e }' )
    return { 'error': 'Could not add new month to table' }

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
      Item = week.toItem()
    )
    # Return the week added to the table
    return { 'week': week }
  except ClientError as e:
    print( f'ERROR addWeek: { e }' )
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
      Item = day.toItem()
    )
    # Return the day added to the table
    return { 'day': day }
  except ClientError as e:
    print( f'ERROR addDay: { e }')
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
      Item = page.toItem()
    )
    # Return the page added to the table
    return { 'page': page }
  except ClientError as e:
    print( f'ERROR addPage: { e }')
    return { 'error': 'Could not add new page to table' }
