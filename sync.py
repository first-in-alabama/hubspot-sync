import requests
from hubspot import HubSpot
from hubspot.crm.objects import SimplePublicObjectInputForCreate
from hubspot.crm.objects.exceptions import ApiException

SEASONS_API = 'https://my.firstinspires.org/usfirstapi/seasons/search'
EVENTS_API = 'https://my.firstinspires.org/usfirstapi/events/search?CountryCode=US&StateProv=AL&Season={season_year}&ProgramCode={program_code}'

FIRST_TO_HUBSPOT_MAP = {
  'JFLL': 'FIRST LEGO League Explore',
  'FLL': 'FIRST LEGO League Challenge',
  'FTC': 'FIRST Tech Challenge',
  'FRC': 'FIRST Robotics Competition'
}

def fetch_seasons() -> dict: 
  seasons = None
  try:
    seasons = { s['ProgramCode']: s['SeasonYearStart'] for s in requests.get(SEASONS_API).json() if s['IsCurrentSeason']}
  except:
    seasons = None

  if seasons is None:
    return
  
  return seasons


def fetch_events(program_code: str, season_year: int) -> list:
  events = None
  try:
    events = requests.get(EVENTS_API.format(program_code=program_code, season_year=season_year)).json()
  except:
    events = None

  if events is None:
    return
  
  return events


def get_known_events(api_client: HubSpot) -> list:
  events = []
  page = None

  while True:
    api_response = api_client.crm.objects.basic_api.get_page(
      object_type='0-421',
      limit=100, 
      archived=False, 
      after=page,
      properties=[
      'event_name',
      'event_venue',
      'event_address_1',
      'event_city',
      'event_postal_code',
      'hs_appointment_start',
      'hs_appointment_end',
      'event_volunteer_url',
      'program',
      'event_code',
      'event_season_year'
    ])

    events.extend(api_response.results)

    try:
        page = api_response.paging.next.after
    except AttributeError:
        print("No next page found, pagination completed.")
        break

  return events


def process_events(api_client: HubSpot, first_events: list, known_events: list):
  if len(known_events) > 0:
    # TODO: Handle updates?
    pass

  for event in first_events:
    hubspot_event = SimplePublicObjectInputForCreate(properties={
      'event_name': event['Name'],
      'event_venue': event['Venue'],
      'event_address_1': event['Address1'],
      'event_city': event['City'],
      'event_postal_code': event['PostalCode'],
      'hs_appointment_start': event['StartDate']['Numeric'],
      'hs_appointment_end': event['EndDate']['Numeric'],
      'event_volunteer_url': '',
      'program': FIRST_TO_HUBSPOT_MAP[event['Type']],
      'event_code': event['Code'],
      'event_season_year': event['Season']
    })

    try:
      api_client.crm.objects.basic_api.create(
        object_type='0-421',
        simple_public_object_input_for_create=hubspot_event
      )
    except ApiException as e:
      print("Exception when creating event: %s\n" % e)


def main():
  api_token = None
  with open('/run/secrets/HUBSPOT_API_TOKEN') as f: api_token = f.read()

  api_client = HubSpot(access_token=api_token)
  known_events = get_known_events(api_client)

  seasons = fetch_seasons()
  if seasons is None:
    print('Could not fetch current seasons')
    return
  
  for program_code, season_year in seasons.items():
    if program_code not in FIRST_TO_HUBSPOT_MAP.keys(): continue

    events = fetch_events(program_code, season_year)
    if events is None:
      print('Unable to find events for', program_code)
      continue
    
    process_events(
      api_client,
      events, 
      [event for event in known_events if event.properties['program'] == FIRST_TO_HUBSPOT_MAP[program_code]]
    )
  print('Sync Complete')

if __name__ == '__main__':
  main()