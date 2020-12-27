import os
import sys
import boto3
import numpy as np
from botocore.exceptions import ClientError
sys.path.append(
  os.path.dirname( os.path.dirname( os.path.abspath( __file__ ) ) )
)
from dynamo.entities import Visit, Visitor, Location, Session, Browser # pylint: disable=wrong-import-position
from dynamo.entities import itemToVisit, itemToSession, itemToVisitor # pylint: disable=wrong-import-position
from dynamo.entities import itemToLocation, itemToBrowser # pylint: disable=wrong-import-position

class DynamoClient:
  def __init__( self, tableName, regionName='us-east-1' ):
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

  def addVisitor( self, visitor ):
    '''Adds a visitor to the table.

    Parameters
    ----------
    visitor : Visitor
      The visitor to be added to the table.

    Returns
    -------
    result : dict
      The result of adding the the visitor to the table.
    '''
    if not isinstance( visitor, Visitor ):
      raise ValueError( 'Must pass a Visitor object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = visitor.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'visitor': visitor }
    except ClientError as e:
      print( f'ERROR addVisitor: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Visitor already in table { visitor }' }
      return { 'error': 'Could not add new visitor to table' }

  def addNewVisitor( self, visitor, location, browsers, visits ):
    '''Adds a new visitor and their details the the table.

    Parameters
    ----------
    visitor : Visitor
      The visitor to be added to the table.
    location : Location
      The visitor's location to be added to the table.
    browsers : list[ Browser ]
      The visitor's browsers to be added to the table.
    visits : list[ Visit ]
      The visits to be added to the table.

    Returns
    -------
    result : dict
      The result of adding the visitor and their attributes to the table.
    '''
    result = self.addVisitor( visitor )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    result = self.addLocation( location )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    result = self.addBrowsers( browsers )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    session = Session(
      visits[0].date,
      visits[0].ip,
      np.mean( pageTimes ) if len( pageTimes ) > 1 else pageTimes[0],
      ( visits[-1].date - visits[0].date ).total_seconds()
    )
    result = self.addSession( session )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    result = self.addVisits( visits )
    if 'error' in result.keys():
      return { 'error': result['error'] }
    return {
      'visitor': visitor, 'location': location, 'browsers': browsers,
      'visits': visits, 'session': session
    }

  def removeVisitor( self, visitor ):
    '''Removes a visitor from the table.

    Parameters
    ----------
    visitor : Visitor
      The visitor to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the visitor from the table.
    '''
    if not isinstance( visitor, Visitor ):
      raise ValueError( 'Must pass a Visitor object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = visitor.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'visitor': visitor }
    except ClientError as e:
      print( f'ERROR removeVisitor: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Visitor not in table { visitor }' }
      return { 'error': 'Could not remove visitor from table' }

  def incrementVisitorSessions( self, visitor ):
    '''Increments the number of sessions a visitor has.

    Parameters
    ----------
    visitor : Visitor
      The visitor to have their number of sessions incremented.

    Returns
    -------
    result : dict
      The result of incrementing the number of sessions the visitor has. This
      could be either an error or the updated visitor.
    '''
    if not isinstance( visitor, Visitor ):
      raise ValueError( 'Must pass a Visitor object' )
    try:
      result = self.client.update_item(
        TableName = self.tableName,
        Key = visitor.key(),
        ConditionExpression = 'attribute_exists(PK)',
        UpdateExpression= 'SET #count = #count + :inc',
        ExpressionAttributeNames = { '#count': 'NumberSessions' },
        ExpressionAttributeValues= { ':inc': { 'N': '1' } },
        ReturnValues= 'ALL_NEW'
      )
      visitor.numberSessions = int(
        result['Attributes']['NumberSessions']['N']
      )
      return { 'visitor': visitor }
    except ClientError as e:
      print( f'ERROR incrementSessions: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': 'Visitor not in table' }
      return {
        'error': 'Could not increment the number of sessions of visitor'
      }

  def getVisitorDetails( self, visitor ):
    '''Gets the visitor and their details from the table.

    Parameters
    ----------
    visitor : Visitor
      The visitor to request from the table.

    Returns
    -------
    result : dict
      The result of requesting the visitor from the table. This contains either
      the error that occurred or the visitor's details.
    '''
    try:
      result = self.client.query(
        TableName = self.tableName,
        KeyConditionExpression = '#pk = :pk',
        ExpressionAttributeNames = { '#pk': 'PK' },
        ExpressionAttributeValues = { ':pk': visitor.pk() },
        ScanIndexForward = True
      )
      if len( result['Items'] ) == 0:
        return { 'error': 'Visitor not in table' }
      data = { 'visits': [], 'browsers': [], 'sessions': [] }
      for item in result['Items']:
        if item['Type']['S'] == 'visitor':
          data['visitor'] = itemToVisitor( item )
        elif item['Type']['S'] == 'visit':
          data['visits'].append( itemToVisit( item ) )
        elif item['Type']['S'] == 'session':
          data['sessions'].append( itemToSession( item ) )
        elif item['Type']['S'] == 'location':
          data['location'] = itemToLocation( item )
        elif item['Type']['S'] == 'browser':
          data['browsers'].append( itemToBrowser( item ) )
        else:
          raise Exception(
            f'''Could not parse type: { item }'''
          )
      return data
    except KeyError as e:
      print( f'ERROR getVisitorDetails: {e}')
      return { 'error': 'Could not get visitor from table' }
    except ClientError as e:
      print( f'ERROR getVisitorDetails: { e }')
      return { 'error': 'Could not get visitor from table' }

  def addLocation( self, location ):
    '''Adds the visitor's location to the table.

    Parameters
    ----------
    location : Location
      The visitor's location to be added to the table.

    Returns
    -------
    result : dict
      The result of adding the visitor's location to the table.
    '''
    if not isinstance( location, Location ):
      raise ValueError( 'Must pass a Location object' )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = location.toItem(),
        ConditionExpression = 'attribute_not_exists(PK)'
      )
      return { 'location': location }
    except ClientError as e:
      print( f'ERROR addLocation: { e }')
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return {
          'error': f'Visitor\'s location is already in table { location }'
        }
      return { 'error': 'Could not add new location to table' }

  def removeLocation( self, location ):
    '''Removes a location from the table.

    Parameters
    ----------
    location : Location
      The location to be removed from the table.

    Returns
    -------
    result : dict
      The result of removing the location from the table.
    '''
    if not isinstance( location, Location ):
      raise ValueError( 'Must pass a Location object' )
    try:
      self.client.delete_item(
        TableName = self.tableName,
        Key = location.key(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      return { 'location': location }
    except ClientError as e:
      print( f'ERROR removeLocation: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Location not in table { location }' }
      return { 'error': 'Could not remove location from table' }

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
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    session = Session(
      visits[0].date,
      visits[0].ip,
      np.mean( pageTimes ) if len( pageTimes ) > 1 else pageTimes[0],
      ( visits[-1].date - visits[0].date ).total_seconds()
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
        else:
          raise Exception( f'Could not parse type: { item }' )
      return data
    except ClientError as e:
      print( f'ERROR getSession: { e }')
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
    pageTimes = [
      visit.timeOnPage for visit in visits
      if isinstance( visit.timeOnPage, float )
    ]
    session = Session(
      visits[0].date,
      visits[0].ip,
      np.mean( pageTimes ) if len( pageTimes ) > 1 else pageTimes[0],
      ( visits[-1].date - visits[0].date ).total_seconds()
    )
    try:
      self.client.put_item(
        TableName = self.tableName,
        Item = session.toItem(),
        ConditionExpression = 'attribute_exists(PK)'
      )
      result = self.addVisits( visits )
      if 'error' in result.keys():
        return { 'error': result['error'] }
      return { 'session': session, 'visits': visits }
    except ClientError as e:
      print( f'ERROR updateSession: { e }' )
      if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return { 'error': f'Session not in table { session }' }
      return { 'error': 'Could not update session in table' }
