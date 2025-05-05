import requests
from hubspot import Client
from hubspot.marketing.events import BatchInputMarketingEventCreateRequestParams, ApiException
from pprint import pprint
SEASONS_API = 'https://my.firstinspires.org/usfirstapi/seasons/search'
EVENTS_API = 'https://my.firstinspires.org/usfirstapi/events/search?CountryCode=US&StateProv=AL&Season={season_year}&ProgramCode={program_code}'
FIRST_PROGRAMS = set([ 'JFLL', 'FLL', 'FTC', 'FRC' ])

'''
Get the current seasons for all FIRST programs
'''
def fetch_first_seasons() -> dict: 
  seasons = None
  try: seasons = { s['ProgramCode']: s['SeasonYearStart'] for s in requests.get(SEASONS_API).json() if s['IsCurrentSeason']}
  except: seasons = None
  return seasons


'''
Get the FIRST events for a given program and season
'''
def fetch_first_events(program_code: str, season_year: int) -> list:
  events = None
  try: events = requests.get(EVENTS_API.format(program_code=program_code, season_year=season_year)).json()
  except: events = None  
  return events


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
Map the data from a FIRST event to the property fields of a HubSpot event.
'''
def map_first_event_properties_to_hubspot(first_event) -> object:
  event_code = first_event['Code']
  event_season_year = first_event['Season']
  event_program = first_event['Type']

  return {
    'eventOrganizer': 'FIRST in Alabama',
    'externalAccountId': event_program + str(event_season_year) + str(event_code),
    'externalEventId': event_program + str(event_season_year) + str(event_code),
    'eventName': first_event['Name'],
    'eventType': event_program,
    'startDateTime': first_event['StartDate']['Numeric'],
    'endDateTime': first_event['EndDate']['Numeric'],
    "customProperties": [
      {
        'name': 'event_code',
        'value': event_code
      },
      {
        'name': 'event_venue',
        'value': first_event['Venue']
      },
      {
        "name": "event_season_year",
        "value": event_season_year
      },
      {
          "name": "event_volunteer_url",
          "value": ''
      },
      {
          "name": "event_city",
          "value": first_event['City']
      },
      {
          "name": "event_postal_code",
          "value": first_event['PostalCode']
      },
      {
          "name": "event_address",
          "value": first_event['Address1']
      }
    ]
  }

def get_custom_property(custom_properties: list, key: str) -> str:
  if custom_properties is None: return None
  match = [entry.value for entry in custom_properties if entry.name == key]
  if len(match) != 1: return None
  return match[0]


'''
Create events that are new and update events that already exist.
Events are unique by a combination of event code, program code, and season year.
'''
def process_events(api_client: Client, first_events: list, hubspot_events: list, program_code: str, season_year: int):  
  # Find the existing HubSpot events that match this batch
  program_hubspot_events = [
    event 
    for event 
    in hubspot_events 
    if event.event_type == program_code 
    and get_custom_property(event.custom_properties, 'event_season_year') == str(season_year)
  ]

  events_to_update = []

  # Get events to update
  for existing_event in program_hubspot_events:
    match = [
      event 
      for event in first_events 
      if event['Code'] == get_custom_property(existing_event.custom_properties, 'event_code')
      and event['Type'] == program_code
      and event['Season'] == season_year
    ]

    if len(match) == 0: continue
    if len(match) > 1:
      print('Too many matches found for', program_code, first_event['Code'], season_year, 'in existing events')
      continue

    first_event = match[0]
    hubspot_event = map_first_event_properties_to_hubspot(first_event)
    hubspot_event['externalEventId'] = existing_event.external_event_id
    hubspot_event['objectId'] = existing_event.object_id
    hubspot_event['eventOrganizer'] = existing_event.event_organizer
    
    events_to_update.append(hubspot_event)
    first_events.remove(first_event)

  events_to_create = []

  # Create new events
  for first_event in first_events:
    hubspot_event =  map_first_event_properties_to_hubspot(first_event)
    events_to_create.append(hubspot_event)

  all_events = events_to_update + events_to_create
  if len(all_events) > 0:
    upsert_batch = BatchInputMarketingEventCreateRequestParams(inputs=all_events)
    try:
      api_client.marketing.events.batch_api.upsert(upsert_batch)
    except ApiException as e:
      print("Exception when calling batch_api->update: %s\n" % e)


def get_hubspot_api_token() -> str:
  api_token = None
  try:
    with open('/run/secrets/HUBSPOT_API_TOKEN') as f: api_token = f.read()
  except:
    api_token = None
  return api_token

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
  
  for program_code, season_year in seasons.items():
    if program_code not in FIRST_PROGRAMS: continue

    first_events = fetch_first_events(program_code, season_year)
    if first_events is None:
      print('Unable to find events for', program_code)
      continue
    
    process_events(
      api_client,
      first_events, 
      hubspot_events,
      program_code,
      season_year
    )
  print('Sync Complete')

if __name__ == '__main__':
  main()