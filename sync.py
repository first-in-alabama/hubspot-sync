from copy import deepcopy
from datetime import datetime
import json
import requests
from hubspot import Client
from hubspot.marketing.events import BatchInputMarketingEventCreateRequestParams, ApiException
SEASONS_API = 'https://my.firstinspires.org/usfirstapi/seasons/search'
ELASTIC_SEARCH_EVENTS_URL = 'https://es02.firstinspires.org/events/_search'
EVENT_QUERY_JSON_PATH = '/app/event_query_json.txt'
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
  return api_token if api_token is None else api_token.strip()


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
Creates an event address from ElasticSearch data
'''
def build_event_location(event: dict) -> str:
  location = ''

  venue = event.get('event_venue', None)
  venue = venue if venue is None else str(venue).strip()
  if venue is not None and len(venue) > 1:
    location += venue
    location += '\n'

  address1 = event.get('event_address1', None)
  address1 = address1 if address1 is None else str(address1).strip()
  if address1 is not None and len(address1) > 1:
    location += address1
    location += '\n'

  address2 = event.get('event_address2', None)
  address2 = address2 if address2 is None else str(address2).strip()
  if address2 is not None and len(address2) > 0:
    location += address2
    location += '\n'

  location_last_line = ''

  city = event.get('event_city', None)
  city = city if city is None else str(city).strip()
  if city is not None and len(city) > 1:
    location_last_line += city
    location_last_line += ', '

  location_last_line += 'Alabama'

  postal_code = event.get('event_postal_code', None)
  postal_code = postal_code if postal_code is None else str(postal_code).strip()
  if postal_code is not None and len(postal_code) > 1:
    location_last_line += postal_code

  location_last_line = location_last_line.strip()

  if len(location_last_line) > 0:
    location += location_last_line

  return location


'''
Get the appropriate volunteer url
'''
def get_volunteer_url(event: dict) -> str:
  express = event.get('express_volunteer_url', None)
  express = express if express is None else str(express).strip()
  if express is not None and len(express) > 0:
    return express
  
  legacy = event.get('dashboard_volunteer_deeplink', None)
  legacy = legacy if legacy is None else str(legacy).strip()
  if legacy is not None and len(legacy) > 0:
    return legacy
  
  return ''
  

'''
Fetch events from ElasticSearch for given season.
'''
def get_elastic_search_events(frc_season: int) -> list:
  queryParameters = { 'size': '200' }
  headers = {'Content-type': 'application/json'}
  body = get_event_query_json(frc_season)
  response = requests.get(ELASTIC_SEARCH_EVENTS_URL, params=queryParameters, data=json.dumps(body), headers=headers).json()
  events = [e['_source'] for e in response['hits']['hits']]

  processed_events = []
  for event in events:
    event_type = event.get('event_type', None)
    if event_type is None: continue
    event_season_year = event.get('event_season', None)
    if event_season_year is None: continue
    event_code = event.get('event_code', None)
    if event_code is None: continue

    event_identifier = event_type + str(event_season_year) + str(event_code)
    event_name = event.get('event_name', None)
    if event_name is None: continue

    start_date_str = event.get('date_start', None)
    end_date_str = event.get('date_end', None)
    if start_date_str is None or end_date_str is None: continue

    start_date = int(datetime.fromisoformat(start_date_str).timestamp() * 1000)
    end_date = int(datetime.fromisoformat(end_date_str).timestamp() * 1000)

    location = build_event_location(event)
    volunteer_url = get_volunteer_url(event)

    processed_events.append({
      'eventOrganizer': 'FIRST in Alabama',
      'externalAccountId': event_identifier,
      'externalEventId': event_identifier,
      'eventName': event_name,
      'eventType': event_type,
      'startDateTime': start_date,
      'endDateTime': end_date,
      'eventUrl': volunteer_url,
      'customProperties': [
        { 'name': 'event_code', 'value': str(event_code) },
        { 'name': 'event_season_year', 'value': event_season_year },
        { 'name': 'event_location', 'value': location }
      ]
    })

  return processed_events


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