import os
import sys
import boto3
import numpy as np
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.data._visitor import _Visitor # pylint: disable=wrong-import-position
from dynamo.data._location import _Location # pylint: disable=wrong-import-position
from dynamo.entities import Visit, Session, Browser # pylint: disable=wrong-import-position
from dynamo.entities import Year, Month, Week, Day, Page # pylint: disable=wrong-import-position
from dynamo.entities import itemToVisit, itemToSession # pylint: disable=wrong-import-position
from dynamo.entities import itemToYear, itemToMonth, itemToWeek, itemToDay # pylint: disable=wrong-import-position
from dynamo.entities import itemToPage # pylint: disable=wrong-import-position

def _chunkList( this_list, size ):
  '''Splits a list into a list of lists.

  Parameters
  ----------
  this_list : list
    The list to be split into different lists.
  size : int
    The size of the sublists

  Returns
  -------
    An iterable that iterates over the sublists.
  '''
  for i in range( 0, len( this_list ), size ):
    yield this_list[i:i + size]

def _pagesToDict( pages ):
  '''Converts a list of pages to a dict of ratios.

  Parameters
  ----------
  pages : list[ str ]
    The list of page visits from other pages or to other pages.

  Returns
  -------
  result : dict
    The ratios of the page visits where the keys are the page slugs and the
    values are the ratios relative to the list of page visits.
  '''
  return {
    (
      'www' if page is None else page
    ): pages.count( page ) / len( pages )
    for page in list( set( pages ) )
  }

class DynamoClient( _Visitor, _Location ):
  '''A class to represent the DynamoDB client.

  Attributes
  ----------
  client : boto3.client
    The boto3 DynamoDB client used to access the table.
  tableName : str
    The name of the DynamoDB table.
  '''
  def __init__( self, tableName, regionName = 'us-east-1' ):
    '''Constructs the necessary attributes for the DynamoDB client object.

    Parameters
    ----------
    tableName : str
      The name of the DynamoDB table.
    regionName : str
      The AWS region to connect to.
    '''
    self.client = boto3.client( 'dynamodb', region_name = regionName )
    self.tableName = tableName

  def addVisit( self, visit ):
    '''Adds a visitor's page visit to the table.

    Parameters
    ----------
    visit : Visit
      The visitor's page visit to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a visitor's page visit to the table.
    '''
    if not isinstance( visit, Visit ):
      raise ValueError( 'Must pass a Visit object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = visit.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'visit': visit }
    except ClientError as e:
      print( f'ERROR addVisit: { e }')
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return {
          'error': f'Visitor\'s page visit is already in table { visit }'
        }
      return { 'error': 'Could not add new page visit to table' }

  def removeVisit( self, visit ):
    '''Removes a visit from the table.

    Parameters
    ----------
    visit : Visit
      The visit to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the visit from the table.
    '''
    if not isinstance( visit, Visit ):
      raise ValueError( 'Must pass a Visit object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = visit.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'visit': visit }
    except ClientError as e:
      print( f'ERROR removeVisit: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Visit not in table { visit }' }
      return { 'error': 'Could not remove visit from table' }

  def addVisits( self, visits ):
    '''Adds a visitor's page visit to the table.

    Parameters
    ----------
    visits : list[ Visit ]
      The visitor's page visits to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a visitor's page visits to the table.
    '''
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list' )
    if any( not isinstance( visit, Visit ) for visit in visits ):
      raise ValueError( 'Must pass Visit objects' )
    try:
      if len( visits ) > 25:
        for sub_visits in _chunkList( visits, 25 ):
          self.client.batch_write_item(
            RequestItems = {
              self.tableName: [
                { 'PutRequest': { 'Item': visit.toItem() } }
                for visit in sub_visits
              ] },
          )
      else:
        self.client.batch_write_item(
          RequestItems = {
            self.tableName: [
              { 'PutRequest': { 'Item': visit.toItem() } }
              for visit in visits
            ] },
        )
      return { 'visits': visits }
    except ClientError as e:
      print( f'ERROR addVisits: { e }')
      return { 'error': 'Could not add new page visits to table' }

  def addSession( self, session ):
    '''Adds a visitor's session to the table.

    Parameters
    ----------
    session : Session
      The visitor's session to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a visitor's session to the table.
    '''
    if not isinstance( session, Session ):
      raise ValueError( 'Must pass a Session object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = session.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'session': session }
    except ClientError as e:
      print( f'ERROR addsession: { e }')
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return {
          'error': f'Visitor\'s session is already in table { session }'
        }
      return { 'error': 'Could not add new page visit to table' }

  def removeSession( self, session ):
    '''Removes a session from the table.

    Parameters
    ----------
    session : Session
      The visit to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the session from the table.
    '''
    if not isinstance( session, Session ):
      raise ValueError( 'Must pass a Session object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = session.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'session': session }
    except ClientError as e:
      print( f'ERROR removeSession: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Session not in table { session }' }
      return { 'error': 'Could not remove session from table' }

  def addBrowser( self, browser ):
    '''Adds a browser to the table.

    Parameters
    ----------
    browser : Browser
      The visitor's browser to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a visitor's browser to the table.
    '''
    if not isinstance( browser, Browser ):
      raise ValueError( 'Must pass a Browser object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = browser.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'browser': browser }
    except ClientError as e:
      print( f'ERROR addBrowser: { e }')
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return {
          'error': f'Visitor\'s browser is already in table { browser }'
        }
      return { 'error': 'Could not add new browser to table' }

  def removeBrowser( self, browser ):
    '''Removes a browser from the table.

    Parameters
    ----------
    browser : Browser
      The browser to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the browser from the table.
    '''
    if not isinstance( browser, Browser ):
      raise ValueError( 'Must pass a Browser object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = browser.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'browser': browser }
    except ClientError as e:
      print( f'ERROR removeBrowser: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Browser not in table { browser }' }
      return { 'error': 'Could not remove browser from table' }

  def addBrowsers( self, browsers ):
    '''Adds browsers to the table.

    Parameters
    ----------
    browsers : list[ Browser ]
      The browsers to be added to the table.

    Returns
    -------
    result : dict
      The result of adding browsers to the table.
    '''
    if not isinstance( browsers, list ):
      raise ValueError( 'Must pass a list' )
    if any( not isinstance( browser, Browser ) for browser in browsers ):
      raise ValueError( 'Must pass Browser objects' )
    try:
      self.client.batch_write_item(
        RequestItems = {
          self.tableName: [
            { 'PutRequest': { 'Item': browser.toItem() } }
            for browser in browsers
          ] },
      )
      return { 'browsers': browsers }
    except ClientError as e:
      print( f'ERROR addBrowsers: { e }')
      return { 'error': 'Could not add new browsers to table' }

  def addNewSession( self, visitor, browsers, visits ):
    '''Adds a new session to the table for the given visitor.

    Parameters
    ----------
    visitor : Visitor
      The returning visitor. They will have their number of sessions
      incremented.
    browsers : list[ Browser ]
      The visitor's browsers to be added to the table.
    visits: list[ Visit ]
      The visits to be added to the table.

    Returns
    -------
    result : dict
      The result of adding a new session for a visitor. This could be either
      the error that occurs or the updated visitor, the browsers added, and the
      visits added to the table.
    '''
    result = self.incrementVisitorSessions( visitor )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    visitor = result['visitor']
    result = self.addBrowsers( browsers )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    # Get all of the seconds per page visit that exist.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    # Calculate the average time the visitor spent on the pages. When there are
    # no page times, there is no average time.
    if len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    elif len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    else:
      averageTime = None
    # Calculate the total time spent in this session. When there is only one
    # visit, there is no total time.
    if len( visits ) == 1:
      totalTime = None
    else:
      totalTime = ( visits[-1].date - visits[0].date ).total_seconds()
    session = Session(
      visits[0].date, visits[0].ip, averageTime, totalTime
    )
    result = self.addSession( session )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    result = self.addVisits( visits )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    return {
      'visitor': visitor, 'browsers': browsers, 'visits': visits,
      'session': session
    }

  def getSessionDetails( self, session ):
    '''Gets the session and visits from the table.

    Parameters
    ----------
    session : Session
      The session requested from the table.

    Returns
    -------
    data : dict
      The result of getting the session from the table. This contains either
      the error that occurred or the session and its visits.
    '''
    try:
      result = self.client.query(
        TableName = self.tableName,
        IndexName = 'GSI2',
        KeyConditionExpression = '#gsi2 = :gsi2',
        ExpressionAttributeNames = { '#gsi2': 'GSI2PK' },
        ExpressionAttributeValues = { ':gsi2': session.gsi2pk() },
        ScanIndexForward = True
      )
      if len( result['Items'] ) == 0:
        return { 'error': 'Session not in table' }
      data = { 'visits': [] }
      for item in result['Items']:
        if item['Type']['S'] == 'visit':
          data['visits'].append( itemToVisit( item ) )
        elif item['Type']['S'] == 'session':
          data['session'] = itemToSession( item )
      return data
    except ClientError as e:
      print( f'ERROR getSessionDetails: { e }')
      return { 'error': 'Could not get session from table' }

  def updateSession( self, session, visits ):
    '''Updates a session with new visits and attributes.

    Parameters
    ----------
    session : Session
      The session to change the average time on page and the total time on the
      website.
    visits : list[ Visit ]
      All of the visits that belong to the session.
    '''
    if not isinstance( session, Session ):
      raise ValueError( 'Must pass a Session object')
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list of Visit objects' )
    if not all( [
      isinstance( visit, Visit ) for visit in visits
    ] ):
      raise ValueError( 'List of visits must be of Visit type' )
    # Get all of the seconds per page visit that exist.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    # Calculate the average time the visitor spent on the pages. When there are
    # no page times, there is no average time.
    if len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    elif len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    else:
      averageTime = None
    # Calculate the total time spent in this session. When there is only one
    # visit, there is no total time.
    if len( visits ) == 1:
      totalTime = None
    else:
      totalTime = ( visits[-1].date - visits[0].date ).total_seconds()
    session = Session(
      visits[0].date, visits[0].ip, averageTime, totalTime
    )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = session.toItem(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      self.addVisits( visits )
      return { 'session': session, 'visits': visits }
    except ClientError as e:
      print( f'ERROR updateSession: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Session not in table { session }' }
      return { 'error': 'Could not update session in table' }

  def addYear( self, visits ):
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
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list' )
    if len( {visit.slug for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same slug' )
    if len( {visit.title for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same title' )
    if len( {visit.date.strftime( '%Y' ) for visit in visits } ) != 1:
      raise ValueError( 'List of visits must be from the same year' )
    # Find all the pages visited before and after this one.
    toPages = [ visit.nextSlug for visit in visits ]
    fromPages = [ visit.prevSlug for visit in visits ]
    # Calculate the average time spent on the page.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    if len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    elif len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    else:
      averageTime = None
    # Create the week object
    year = Year(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y' ),
      len( { visit.ip for visit in visits } ),
      averageTime,
      toPages.count( None ) / len( toPages ),
      _pagesToDict( fromPages ),
      _pagesToDict( toPages )
    )
    try:
      # Add the year to the table
      self.client.put_item( TableName = self.tableName, Item = year.toItem() )
      # Return the year added to the table
      return { 'year': year }
    except ClientError as e:
      print( f'ERROR addYear: { e }' )
      return { 'error': 'Could not add new year to table' }

  def addMonth( self, visits ):
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
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list' )
    if len( {visit.slug for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same slug' )
    if len( {visit.title for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same title' )
    if len( {visit.date.strftime( '%Y-%m' ) for visit in visits } ) != 1:
      raise ValueError( 'List of visits must be from the same year and month' )
    # Find all the pages visited before and after this one.
    toPages = [ visit.nextSlug for visit in visits ]
    fromPages = [ visit.prevSlug for visit in visits ]
    # Calculate the average time spent on the page.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    if len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    elif len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    else:
      averageTime = None
    # Create the week object
    month = Month(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y-%m' ),
      len( { visit.ip for visit in visits } ),
      averageTime,
      toPages.count( None ) / len( toPages ),
      _pagesToDict( fromPages ),
      _pagesToDict( toPages )
    )
    try:
      # Add the month to the table
      self.client.put_item( TableName = self.tableName, Item = month.toItem() )
      # Return the month added to the table
      return { 'month': month }
    except ClientError as e:
      print( f'ERROR addMonth: { e }' )
      return { 'error': 'Could not add new month to table' }

  def addWeek( self, visits ):
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
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list' )
    if len( {visit.slug for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same slug' )
    if len( {visit.title for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same title' )
    if len( {visit.date.strftime( '%Y-%U' ) for visit in visits } ) != 1:
      raise ValueError( 'List of visits must be from the same year and week' )
    # Find all the pages visited before and after this one.
    toPages = [ visit.nextSlug for visit in visits ]
    fromPages = [ visit.prevSlug for visit in visits ]
    # Calculate the average time spent on the page.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    if len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    elif len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    else:
      averageTime = None
    # Create the week object
    week = Week(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y-%U' ),
      len( { visit.ip for visit in visits } ),
      averageTime,
      toPages.count( None ) / len( toPages ),
      _pagesToDict( fromPages ),
      _pagesToDict( toPages )
    )
    try:
      # Add the week to the table
      self.client.put_item( TableName = self.tableName, Item = week.toItem() )
      # Return the week added to the table
      return { 'week': week }
    except ClientError as e:
      print( f'ERROR addWeek: { e }' )
      return { 'error': 'Could not add new week to table' }

  def addDay( self, visits ):
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
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list' )
    if len( {visit.slug for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same slug' )
    if len( {visit.title for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same title' )
    if len( {visit.date.strftime( '%Y-%m-%d' ) for visit in visits } ) != 1:
      raise ValueError(
        'List of visits must be from the same year, month, and day'
      )
    # Find all the pages visited before and after this one.
    toPages = [ visit.nextSlug for visit in visits ]
    fromPages = [ visit.prevSlug for visit in visits ]
    # Calculate the average time spent on the page.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    if len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    elif len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    else:
      averageTime = None
    # Create the day object.
    day = Day(
      visits[0].slug,
      visits[0].title,
      visits[0].date.strftime( '%Y-%m-%d' ),
      len( { visit.ip for visit in visits } ),
      averageTime,
      toPages.count( None ) / len( toPages ),
      _pagesToDict( fromPages ),
      _pagesToDict( toPages )
    )
    try:
      # Add the day to the table
      self.client.put_item( TableName = self.tableName, Item = day.toItem() )
      # Return the day added to the table
      return { 'day': day }
    except ClientError as e:
      print( f'ERROR addDay: { e }')
      return { 'error': 'Could not add new day to table' }

  def addPage( self, visits ):
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
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list' )
    if len( {visit.slug for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same slug' )
    if len( {visit.title for visit in visits } ) != 1:
      raise ValueError( 'List of visits must have the same title' )
    # Find all the pages visited before and after this one.
    toPages = [ visit.nextSlug for visit in visits ]
    fromPages = [ visit.prevSlug for visit in visits ]
    # Calculate the average time spent on the page.
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    if len( pageTimes ) > 1:
      averageTime = np.mean( pageTimes )
    elif len( pageTimes ) == 1:
      averageTime = pageTimes[0]
    else:
      averageTime = None
    # Create the page object.
    page = Page(
      visits[0].slug,
      visits[0].title,
      len( { visit.ip for visit in visits } ),
      averageTime,
      toPages.count( None ) / len( toPages ),
      _pagesToDict( fromPages ),
      _pagesToDict( toPages )
    )
    try:
      # Add the page to the table
      self.client.put_item( TableName = self.tableName, Item = page.toItem() )
      # Return the page added to the table
      return { 'page': page }
    except ClientError as e:
      print( f'ERROR addPage: { e }')
      return { 'error': 'Could not add new page to table' }

  def updatePage( self, visits ):
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
      could be either the error that occurs or the new page, days, weeks,
      months, and years.
    '''
    if not isinstance( visits, list ):
      raise ValueError( 'Must pass a list of Visit objects' )
    if not all( [
      isinstance( visit, Visit ) for visit in visits
    ] ):
      raise ValueError( 'List of visits must be of Visit type' )
    # Add each day's visits
    days = []
    for day_visits in [
      [ visit for visit in visits if visit.date.strftime( '%Y-%m-%d' ) == day ]
      for day in { visit.date.strftime( '%Y-%m-%d' ) for visit in visits }
    ]:
      day_result = self.addDay( day_visits )
      if 'error' in day_result.keys():
        return { 'error': day_result['error'] }
      days.append( day_result['day'] )
    # Add each week's visits
    weeks = []
    for week_visits in [
      [ visit for visit in visits if visit.date.strftime( '%Y-%U' ) == week ]
      for week in { visit.date.strftime( '%Y-%U' ) for visit in visits }
    ]:
      week_result = self.addWeek( week_visits )
      if 'error' in week_result.keys():
        return { 'error': week_result['error'] }
      days.append( week_result['week'] )
    # Add each months's visits
    months = []
    for month_visits in [
      [ visit for visit in visits if visit.date.strftime( '%Y-%m' ) == month ]
      for month in { visit.date.strftime( '%Y-%m' ) for visit in visits }
    ]:
      month_result = self.addMonth( month_visits )
      if 'error' in month_result.keys():
        return { 'error': month_result['error'] }
      months.append( month_result['month'] )
    # Add each years's visits
    years = []
    for year_visits in [
      [ visit for visit in visits if visit.date.strftime( '%Y' ) == year ]
      for year in { visit.date.strftime( '%Y' ) for visit in visits }
    ]:
      year_result = self.addYear( year_visits )
      if 'error' in year_result.keys():
        return { 'error': year_result['error'] }
      years.append( year_result['year'] )
    # Add the page to the table
    page_result = self.addPage( visits )
    if 'error' in page_result.keys():
      return { 'error': page_result['error'] }
    return {
      'page': page_result['page'], 'days': days, 'weeks': weeks,
      'months': months, 'years': years
    }

  def getPageDetails( self, page ):
    '''Gets a page and its days, weeks, months, and years of analytics.

    Parameters
    ----------
    page : Page
      The page to request the details of.

    Raises
    ------
    Exception
      When the items returned by the query are not either a visit, session,
      page, day, week, month, or year.
    KeyError
      When the items returned are not structured as expected
    ClientError
      When the DynamoDB client raises an exception.

    Returns
    -------
    result : dict
      The result of requesting the page from the table. This contains either the
      error that occurred or the page's analytics.
    '''
    if not isinstance( page, Page ):
      raise ValueError( 'Must pass a Page object' )
    try:
      result = self.client.query(
        TableName = self.tableName,
        IndexName = 'GSI1',
        KeyConditionExpression = '#gsi1 = :gsi1',
        ExpressionAttributeNames = { '#gsi1': 'GSI1PK' },
        ExpressionAttributeValues = { ':gsi1': page.gsi1pk() },
        ScanIndexForward = True
      )
      # Return the error when there are no items returned from the table.
      if len( result['Items'] ) == 0:
        return { 'error': 'Page not in table' }
      # Use a dictionary to store the items returned from the table
      data = {
        'visits': [], 'days': [], 'weeks': [], 'months': [], 'years': []
      }
      for item in result['Items']:
        if item['Type']['S'] == 'visit':
          data['visits'].append( itemToVisit( item ) )
        elif item['Type']['S'] == 'page':
          data['page'] = itemToPage( item )
        elif item['Type']['S'] == 'day':
          data['days'].append( itemToDay( item ) )
        elif item['Type']['S'] == 'week':
          data['weeks'].append( itemToWeek( item ) )
        elif item['Type']['S'] == 'month':
          data['months'].append( itemToMonth( item ) )
        elif item['Type']['S'] == 'year':
          data['years'].append( itemToYear( item ) )
      return data
    except ClientError as e:
      print( f'ERROR getPageDetails: { e }')
      return { 'error': 'Could not get page from table' }
