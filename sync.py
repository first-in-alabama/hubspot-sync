from copy import deepcopy
from datetime import datetime
import json
import requests
from hubspot import Client
from hubspot.marketing.events import BatchInputMarketingEventCreateRequestParams, ApiException
SEASONS_API = 'https://my.firstinspires.org/usfirstapi/seasons/search'
ELASTIC_SEARCH_EVENTS_URL = 'https://es02.firstinspires.org/events/_search'
EVENT_QUERY_JSON_PATH = 'event_query_json.txt'
TOKEN_PATH = '/run/secrets/HUBSPOT_API_TOKEN'


'''
Get the current seasons for all FIRST programs
'''
def fetch_first_seasons() -> dict: 
  seasons = None
  try: seasons = { s['ProgramCode']: int(s['SeasonYearStart']) for s in requests.get(SEASONS_API).json() if s['IsCurrentSeason']}
  except: seasons = None
  return seasons


'''
Get all events HubSpot currently knows
'''
def get_known_events(api_client: Client) -> list:
  events = []
  page = None

  while True:
    api_response = api_client.marketing.events.basic_api.get_all(
      limit=100, 
      after=page
    )

    events.extend(api_response.results)

    try:
        page = api_response.paging.next.after
    except AttributeError:
        print("No next page found, pagination completed.")
        break

  return events

'''
Lookup a custom property value on the HubSpot object
'''
def get_custom_property(custom_properties: list, key: str) -> str:
  if custom_properties is None: return None
  match = [entry.value for entry in custom_properties if entry.name == key]
  if len(match) != 1: return None
  return match[0]


'''
Create events that are new and update events that already exist.
Events are unique by a combination of event code, program code, and season year.
'''
def process_events(api_client: Client, first_events: list, hubspot_events: list, frc_season_year: int):  
  # Find the existing HubSpot events that match for the season
  existing_hubspot_events = [
    event 
    for event 
    in hubspot_events 
    if (event.event_type == 'FRC' and get_custom_property(event.custom_properties, 'event_season_year') == str(frc_season_year))
    or (event.event_type != 'FRC' and get_custom_property(event.custom_properties, 'event_season_year') == str(frc_season_year - 1))
  ]

  events_to_update = []

  # Get events to update
  for existing_event in existing_hubspot_events:
    match = [
      event 
      for event in first_events 
      if event['externalEventId'] == existing_event.external_event_id
    ]

    if len(match) == 0: continue
    if len(match) > 1:
      print('Too many matches found for', existing_event.external_event_id, 'in existing events')
      continue

    first_event = deepcopy(match[0])
    first_event['externalEventId'] = existing_event.external_event_id
    first_event['objectId'] = existing_event.object_id
    first_event['eventOrganizer'] = existing_event.event_organizer
    
    events_to_update.append(first_event)
    first_events.remove(match[0])

  # Create remaining events
  events_to_create = deepcopy(first_events)

  all_events = events_to_update + events_to_create
  if len(all_events) > 0:
    upsert_batch = BatchInputMarketingEventCreateRequestParams(inputs=all_events)
    try:
      api_client.marketing.events.batch_api.upsert(upsert_batch)
    except ApiException as e:
      print("Exception when calling batch_api->update: %s\n" % e)


'''
Extract token from secrets file.
'''
def get_hubspot_api_token() -> str:
  api_token = None
  try:
    with open(TOKEN_PATH) as f: api_token = f.read()
  except:
    api_token = None
  return api_token


'''
Extract event query from file.
'''
def get_event_query_json(frc_season: int) -> dict:
  content = None
  with open(EVENT_QUERY_JSON_PATH) as f:
    content = f.read()
  
  if content is None: return {}
  content = content.format(frc_season, frc_season - 1)
  return json.loads(content)


'''
Fetch events from ElasticSearch for given season.
'''
def get_elastic_search_events(frc_season: int) -> dict:
  queryParameters = { 'size': '200' }
  headers = {'Content-type': 'application/json'}
  body = get_event_query_json(frc_season)
  response = requests.get(ELASTIC_SEARCH_EVENTS_URL, params=queryParameters, data=json.dumps(body), headers=headers).json()
  events = [e['_source'] for e in response['hits']['hits']]

  return [
    {
      'eventOrganizer': 'FIRST in Alabama',
      'externalAccountId': e.get('event_type', '') + str(e.get('event_season', 0)) + str(e.get('event_code', '')),
      'externalEventId': e.get('event_type', '') + str(e.get('event_season', 0)) + str(e.get('event_code', '')),
      'eventName': e.get('event_name', ''),
      'eventType': e.get('event_type', ''),
      'startDateTime': int(datetime.fromisoformat(e.get('date_start', '1970-01-01')).timestamp() * 1000),
      'endDateTime': int(datetime.fromisoformat(e.get('date_end', '1970-01-01')).timestamp() * 1000),
      "customProperties": [
        { 'name': 'event_code', 'value': str(e.get('event_code', '')) },
        { 'name': 'event_venue', 'value': e.get('event_venue', '') },
        { "name": "event_season_year", "value": e.get('event_season', 0) },
        { "name": "event_volunteer_url", "value": e.get('event_volunteer_url', None) or e.get('dashboard_volunteer_deeplink', '') },
        { "name": "event_city", "value": e.get('event_city', '') },
        { "name": "event_postal_code", "value": e.get('event_postal_code', '') },
        { "name": "event_address", "value": e.get('event_address', '') }
      ]
    }
    for e 
    in events]


'''
Perform sync.
'''
def main():
  print('Sync Begin')

  api_token = get_hubspot_api_token()
  if api_token is None:
    print('Could not retrieve HubSpot API token')
    return

  api_client = Client.create(access_token=api_token)
  hubspot_events = get_known_events(api_client)

  seasons = fetch_first_seasons()
  if seasons is None:
    print('Could not fetch current seasons')
    return
  
  frc_season = seasons.get('FRC', None)
  if frc_season is None:
    print('Unable to determine FRC season')
    return
  
  first_events = get_elastic_search_events(frc_season)
  if first_events is None:
    print('Unable to find events for the', frc_season, 'FIRST season')
    return
  
  process_events(
    api_client,
    first_events, 
    hubspot_events,
    frc_season
  )
  
  print('Sync Complete')

if __name__ == '__main__':
  main()