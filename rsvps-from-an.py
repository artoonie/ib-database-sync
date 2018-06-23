'''
Fetch the RSVPs from a given event in ActionNetwork and import them into
Airtable.

'''

import requests
import argparse
from collections import Counter
import pprint
import maya
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--an-title', help='Event title (public) in Action Network')
parser.add_argument('--an-name', default='', help='Event name (admin) '
        'in Action Network')
parser.add_argument('--airtable-name', help='Event name in Airtable '
        '(<name> <date>)')
parser.add_argument('--attendees', action='store_true',
        help='Action Network resource is sign-in form')
parser.add_argument('-f', '--force', help='Do not ask for confirmation')

args = parser.parse_args()

AN_Token = 'xxxx'
AN_Entrypoint = 'https://actionnetwork.org/api/v2/'

Airtable_Token = 'xxxx'
Airtable_Entrypoint = 'https://api.airtable.com/v0/appKBM2llidtAm4kw/'

event_title = args.an_title
event_name = args.an_name
Airtable_event_name = args.airtable_name

AN_header = {'OSDI-API-Token': AN_Token}
Airtable_header = {'Authorization': 'Bearer %s' % Airtable_Token}

if args.attendees:
    AN_response = requests.get(AN_Entrypoint + 'forms', headers=AN_header)
    json_response = AN_response.json()

    for form in json_response['_embedded']['osdi:forms']:
        if form['title'] == event_title and form['name'] == event_name:
            print('Found form!')
            print('Title: %s' % form['title'])
            print('Administrative name: %s' % form['name'])
            print(maya.MayaDT.from_iso8601(form['created_date']).rfc2822())
            proceed = input('Proceed with transfer? y/[n] ')
            if proceed not in 'Yy':
                print('Aborting...')
                sys.exit(0)
            for identifier in form['identifiers']:
                if identifier[:14] == 'action_network':
                    form_id = identifier[15:]
            submissions_link = form['_links']['osdi:submissions']['href']

    AN_response = requests.get(submissions_link, headers=AN_header)
    json_response = AN_response.json()
    rsvp_ids = []
    while json_response['page'] <= json_response['total_pages']:
        rsvp_ids.extend(x['action_network:person_id'] for x in
                json_response['_embedded']['osdi:submissions'])
        AN_response = requests.get(json_response['_links']['next']['href'],
                headers=AN_header)
        json_response = AN_response.json()
else:
    AN_response = requests.get(AN_Entrypoint + 'events', headers=AN_header)
    json_response = AN_response.json()

    for event in json_response['_embedded']['osdi:events']:
        if event['title'] == event_title and event['name'] == event_name:
            print('Found event!')
            print('Title: %s' % event['title'])
            print('Administrative name: %s' % event['name'])
            print(maya.MayaDT.from_iso8601(event['start_date']).rfc2822())
            proceed = input('Proceed with transfer? y/[n] ')
            if proceed not in 'Yy':
                print('Aborting...')
                sys.exit(0)
            for identifier in event['identifiers']:
                if identifier[:14] == 'action_network':
                    event_id = identifier[15:]
            attendances_link = event['_links']['osdi:attendances']['href']

    AN_response = requests.get(attendances_link, headers=AN_header)
    json_response = AN_response.json()
    rsvp_ids = []
    while json_response['page'] <= json_response['total_pages']:
        rsvp_ids.extend(x['action_network:person_id'] for x in
                json_response['_embedded']['osdi:attendances'])
        AN_response = requests.get(json_response['_links']['next']['href'],
                headers=AN_header)
        json_response = AN_response.json()


Airtable_params = {'fields[]': ['Name', 'AN unique ID'],
        'filterByFormula': 'FIND({AN unique ID}, "' +
        ';'.join(rsvp_ids) + '")'}
Airtable_response = requests.get(Airtable_Entrypoint +
        'Community%20Members', params=Airtable_params,
        headers=Airtable_header)
json_response = Airtable_response.json()
retrieved_ids = [record['fields']['AN unique ID'] for record in
        json_response['records']]
new_ids = []
for rsvp_id in rsvp_ids:
    if rsvp_id not in retrieved_ids:
        new_ids.append(rsvp_id)

for new_id in new_ids:
    AN_response = requests.get(AN_Entrypoint + 'people/' + new_id,
            headers=AN_header)
    json_response = AN_response.json()
    if json_response.get('postal_addresses', None) is not None:
        full_address = json_response['postal_addresses'][0]
        address = ', '.join(full_address.get('address_lines', []))
        city = full_address.get('locality', '')
        zipcode = full_address.get('postal_code', '')
    else:
        address = ''
        city = ''
        zipcode = ''

    new_record = {'fields': {
        'First Name': json_response.get('given_name', '').strip(),
        'Last Name': json_response.get('family_name', '').strip(),
        'Email Address': json_response['email_addresses'][0]['address'],
        'AN unique ID': new_id,
        'Address': address,
        'City': city,
        'Zip code': zipcode,
        }
        }
    Airtable_header['Content-type'] = 'application/json'
    Airtable_response = requests.post(Airtable_Entrypoint +
            'Community%20Members', headers=Airtable_header,
            json=new_record)
    del Airtable_header['Content-type']

# Fetch the event from Airtable
Airtable_params = {'fields[]': ['Event'],
        'filterByFormula': '{Event} = "%s"' % Airtable_event_name}
Airtable_response = requests.get(Airtable_Entrypoint + 'Events',
        params=Airtable_params, headers=Airtable_header)
json_response = Airtable_response.json()
pprint.pprint(json_response)
record_id = json_response['records'][0]['id']
Airtable_params = {'fields[]': ['Name'],
        'filterByFormula': 'FIND({AN unique ID}, "' +
        ';'.join(rsvp_ids) + '")'}
Airtable_response = requests.get(Airtable_Entrypoint +
        'Community%20Members', params=Airtable_params,
        headers=Airtable_header)
json_response = Airtable_response.json()
if args.attendees:
    table_name = 'Event%20Attendance'
else:
    table_name = 'Event%20RSVPs'
for record in json_response['records']:
    # Add new entry for event RSVPs
    new_record = {'fields': {
        'Name link': [record['id']],
        'Event': [record_id],
        }
        }
    Airtable_header['Content-type'] = 'application/json'
    Airtable_response = requests.post(Airtable_Entrypoint +
            table_name, json=new_record, headers=Airtable_header)
    del Airtable_header['Content-type']
