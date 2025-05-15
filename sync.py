from copy import deepcopy
from datetime import datetime
import json
import requests
from hubspot import Client
from hubspot.marketing.events import BatchInputMarketingEventCreateRequestParams, ApiException
SEASONS_API = 'https://my.firstinspires.org/usfirstapi/seasons/search'
ELASTIC_SEARCH_EVENTS_URL = 'https://es02.firstinspires.org/events/_search'
FIRST_PROGRAMS = set([ 'JFLL', 'FLL', 'FTC', 'FRC' ])

'''
Get the current seasons for all FIRST programs
'''
def fetch_first_seasons() -> list: 
  seasons = None
  try: seasons = { s['ProgramCode']: s['SeasonYearStart'] for s in requests.get(SEASONS_API).json() if s['IsCurrentSeason']}
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


def get_hubspot_api_token() -> str:
  api_token = None
  try:
    with open('/run/secrets/HUBSPOT_API_TOKEN') as f: api_token = f.read()
  except:
    api_token = None
  return api_token

def get_elastic_search_events(frc_season: int) -> dict:
  queryParameters = { 'size': '200' }
  headers = {'Content-type': 'application/json'}
  body = {
    "query": {
      "bool": {
        "must": [
          {
            "match": {
              "countryCode": "US"
            }
          },
          {
            "match": {
              "event_stateprov": "AL"
            }
          },
          {
            "bool": {
              "should": [
                {
                  "bool": {
                    "must": [
                      {
                        "match": {
                          "event_season": frc_season
                        }
                      },
                      {
                        "match": {
                          "event_type": "FRC"
                        }
                      }
                    ]
                  }
                },
                {
                  "bool": {
                    "must": [
                      {
                        "match": {
                          "event_season": frc_season - 1
                        }
                      },
                      {
                        "bool": {
                          "must_not": [
                            {
                              "match": {
                                "event_type": "FRC"
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    }
  }

  response = requests.get(ELASTIC_SEARCH_EVENTS_URL, params=queryParameters, data=json.dumps(body), headers=headers).json()
  events = [e['_source'] for e in response['hits']['hits']]
  return [
    {
      'eventOrganizer': 'FIRST in Alabama',
      'externalAccountId': str(e['event_type']) + str(e['event_season']) + str(e['event_code']),
      'externalEventId': str(e['event_type']) + str(e['event_season']) + str(e['event_code']),
      'eventName': e['event_name'],
      'eventType': e['event_type'],
      'startDateTime': int(datetime.fromisoformat(e['date_start']).timestamp() * 1000),
      'endDateTime': int(datetime.fromisoformat(e['date_end']).timestamp() * 1000),
      "customProperties": [
        {
          'name': 'event_code',
          'value': str(e['event_code'])
        },
        {
          'name': 'event_venue',
          'value': e['event_venue']
        },
        {
          "name": "event_season_year",
          "value": e['event_season']
        },
        {
            "name": "event_volunteer_url",
            "value": ''
        },
        {
            "name": "event_city",
            "value": e['event_city']
        },
        {
            "name": "event_postal_code",
            "value": e['event_postalcode']
        },
        {
            "name": "event_address",
            "value": e['event_address1']
        }
      ]
    }
    for e 
    in events]


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
  
  frc_season = seasons['FRC']
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