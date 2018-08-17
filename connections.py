"""
Connections to ActionNetwork and AirTable.
"""

from math import sin, cos, sqrt, atan2, radians
import os
import requests
import time
import timeago

import hashlib
import json
from records import HashFriendlyMember

class Connection(object):
    def __init__(self, verbose):
        self.verbose = verbose

    def request(self, href, params, headers):
        try_count = 0
        while try_count < 3:
            try:
                response = requests.get(href,
                                        params = params,
                                        headers = headers)
                return response.json()
            except Exception as e:
                try_count += 1
        raise RuntimeError("Tried thrice and failed to get url " + href +\
                           "because of " + e.message)

    def make_request(self, href, params, header, cache_filename=None):
        """ set cache_filename to enable caching of this result """
        if cache_filename is None or not os.path.exists(cache_filename):
            if self.verbose:
                print("Requesting " + href)
            json_response = self.request(href,
                                         params = params,
                                         headers = header)
            time.sleep(0.2) # Respect AirTable rate limiting rules
            if cache_filename:
                cache_directory = os.path.dirname(cache_filename)
                if not os.path.exists(cache_directory):
                    os.makedirs(cache_directory)
                with open(cache_filename, 'w') as cached_json_fp:
                    json.dump(json_response, cached_json_fp,
                              sort_keys=True, indent=4)
        else:
            timestamp = os.path.getctime(cache_filename)
            ago = timeago.format(timestamp)
            if self.verbose:
                print("Loading %s from cache downloaded %s" % \
                      (cache_filename, ago))
            with open(cache_filename, 'r') as cached_json_fp:
                json_response = json.load(cached_json_fp)

        return json_response

class ANConnection(Connection):
    def __init__(self, an_token, verbose):
        super(ANConnection, self).__init__(verbose)
        self.header = {'OSDI-API-Token': an_token}
        self.params = None
        self.href = 'https://actionnetwork.org/api/v2/people'
        self.num_members_filtered = 0

    def _create_member_from(self, member_json):
        address = member_json['postal_addresses'][0]
        return HashFriendlyMember(
            email_address = member_json['email_addresses'][0]['address'],
            last_edit     = member_json['modified_date'],
            first_name    = member_json.get('given_name'),
            last_name     = member_json.get('family_name'),
            zip_code      = address.get('postal_code'),
            unique_id     = member_json['identifiers'][0],
            source_name   = "ActionNetwork")

    def _filter_unimportant(self, members_json):
        # Far away members who signed up to watch the Lakoff/Hochchild stream
        # are not important and should really be purged from the database.
        # Until then, we need to filter them out explicitly.
        naughty_url = 'https://actionnetwork.org/forms/'\
                      'live-stream-of-reaching-out-to-trump-voters'

        def cache_filename_from_url(url):
            splitter = 'actionnetwork.org/api/v2/'
            cache_filename = url.split(splitter)[1]
            return cache_filename.replace('/', '-')
        def km_from_berkeley(lat, lon):
            # https://stackoverflow.com/questions/19412462/
            # getting-distance-between-two-points-based-on-latitude-longitude
            R = 6373.0

            lat1 = radians(abs(lat))
            lon1 = radians(abs(lon))
            lat2 = radians(abs(37.872483))
            lon2 = radians(abs(-122.266359))

            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))

            distance = R * c
            return distance

        def did_only_fill_out_form(member):
            forms_href = member['_links']['osdi:submissions']['href']
            cache_filename = cache_filename_from_url(forms_href)
            assert isinstance(cache_filename, basestring)

            # 1. Download the "submissions" list
            forms_json = self.make_request(
                href = forms_href,
                params = self.params,
                header = self.header,
                cache_filename = 'cache/an-submissions/' + cache_filename)
            submissions =  forms_json['_embedded']['osdi:submissions']

            if len(submissions) != 1:
                return False

            # 2. Download the "form" to see what's inside
            submission = submissions[0]
            form_href = submission['_links']['osdi:form']['href']
            cache_filename = cache_filename_from_url(form_href)
            form_json = self.make_request(
                href = form_href,
                params = self.params,
                header = self.header,
                cache_filename = 'cache/an-forms/' + cache_filename)

            try:
                return form_json['browser_url'] == naughty_url
            except KeyError as e:
                if self.verbose:
                    print "While trying to download form", form_href
                    print "We received an error:", e.message
                    print "The data returned from the server is:"
                    print form_json
                return False

        def is_far_away(member):
            loc = member['postal_addresses'][0]['location']
            lat = loc['latitude']
            lon = loc['longitude']
            if lat is None or lon is None:
                # Err on the side of keeping a member whose address is unknown
                return False
            return km_from_berkeley(float(lat), float(lon)) > 50

        filtered_members = [m for m in members_json if
                       not is_far_away(m) or not did_only_fill_out_form(m)]
        #for m in members_json:
        #    if not is_far_away(m):
        #        print "member is not far away"
        #    if not did_only_fill_out_form(m):
        #        print "member filled out more than just one"
        self.num_members_filtered += len(members_json)-len(filtered_members)
        return filtered_members

    def _create_members_from(self, an_json):
        members_json = an_json['_embedded']['osdi:people']
        members_json = self._filter_unimportant(members_json)
        return [self._create_member_from(x) for x in members_json]

    def create_members(self):
        members = []
        page = 0
        href = self.href
        while True:
            an_json = self.make_request(
                href = href,
                params = self.params,
                header = self.header,
                cache_filename = 'cache/an_cache_%s.json' % page)
            members.extend(self._create_members_from(an_json))

            href = an_json
            if 'next' not in an_json['_links']:
                return members

            href = an_json['_links']['next']['href']
            page += 1
            assert page < 500 # safety check

class ATConnection(Connection):
    fields_to_request = ('Email Address',
                         'First Name',
                         'Last Name',
                         'Zip code',
                         'id')

    def __init__(self, at_token, verbose):
        super(ATConnection, self).__init__(verbose)
        self.header = {'Authorization': 'Bearer %s' % at_token}
        self.params = {'fields': self.fields_to_request}
        self.href = 'https://api.airtable.com/v0/appKBM2llidtAm4kw/'\
                    'Community%20Members'

    def _create_member_from(self, member_json):
        # TODO: Can we get the modified time instead of created?
        fields = member_json['fields']
        last_edit = fields.get('createdTime', None)

        return HashFriendlyMember(
            email_address = fields.get(self.fields_to_request[0]),
            first_name    = fields.get(self.fields_to_request[1]),
            last_name     = fields.get(self.fields_to_request[2]),
            zip_code      = fields.get(self.fields_to_request[3]),
            last_edit     = last_edit,
            unique_id     = member_json["id"],
            source_name   = "AirTable")


    def _create_members_from(self, at_json):
        members_json = at_json['records']
        return [self._create_member_from(x) for x in members_json]

    def create_members(self):
        members = []
        page = 0
        params = dict(self.params) # make a copy
        while True:
            at_json = self.make_request(
                              href = self.href,
                              params = params,
                              header = self.header,
                              cache_filename = 'cache/at_cache_%s.json' % page)
            members.extend(self._create_members_from(at_json))

            if 'offset' not in at_json:
                return members

            params['offset'] = at_json['offset']
            page += 1
            assert page < 500 # safety check