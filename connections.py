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
from actions import CreateAction, UpdateAction, DeleteAction

class Connection(object):
    def __init__(self, verbose):
        self.verbose = verbose

    def _request_helper(self, href, params, headers, action=requests.get, data=None):
        kwargs = {'params': params, 'headers': headers}
        if data is not None:
            kwargs['data'] = json.dumps(data)
            kwargs['headers']['Content-Type'] = 'application/json'

        try_count = 0
        while try_count < 3:
            try:
                response = action(href, **kwargs)

                if response.status_code != 200:
                    raise RuntimeError("Server error %d: %s\n%s" % 
                          (response.status_code, response.text, href))

                return response.json()
            except requests.exceptions.RequestException as e:
                try_count += 1
        raise RuntimeError("Server errored thrice for URL %s:\n>> %s" %
                           (href, e.message))

    def post_request(self, href, params, headers, data):
        return self._request_helper(href, params, headers, requests.post, data)

    def put_request(self, href, params, headers, data):
        return self._request_helper(href, params, headers, requests.put, data)

    def patch_request(self, href, params, headers, data):
        return self._request_helper(href, params, headers, requests.patch, data)

    def get_request(self, href, params, headers):
        return self._request_helper(href, params, headers, requests.get)

    def delete_request(self, href, params, headers):
        return self._request_helper(href, params, headers, requests.delete)

    def make_request(self, href, params, headers, cache_filename=None):
        """ set cache_filename to enable caching of this result """
        if cache_filename is None or not os.path.exists(cache_filename):
            if self.verbose:
                print("Requesting " + href)
            json_response = self.get_request(href,
                                            params = params,
                                            headers = headers)
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

    # Each connection must know how to do any action
    def do_action(self, action):
        """ Create actions return the Member that was created.
            Other actions return nothing. """
        if isinstance(action, CreateAction):
            return self._do_action_create(action)
        elif isinstance(action, UpdateAction):
            return self._do_action_update(action)
        elif isinstance(action, DeleteAction):
            return self._do_action_delete(action)
        else: assert False

    def _do_action_create(self, action): assert False
    def _do_action_update(self, action): assert False
    def _do_action_update(self, action): assert False

class ANConnection(Connection):
    def __init__(self, an_token, verbose):
        super(ANConnection, self).__init__(verbose)
        self.headers = {'OSDI-API-Token': an_token}
        self.params = {}
        self.href = 'https://actionnetwork.org/api/v2/people'

    def _json_to_member(self, member_json):
        address = member_json['postal_addresses'][0]
        return HashFriendlyMember(
            email_address = member_json['email_addresses'][0]['address'],
            last_edit     = member_json['modified_date'],
            first_name    = member_json.get('given_name'),
            last_name     = member_json.get('family_name'),
            zip_code      = address.get('postal_code'),
            unique_id     = member_json['identifiers'][0],
            source_name   = "ActionNetwork")

    def _member_to_json(self, member):
        """ Allows you to pass in a Member or a dictionary, and
            only returns JSON fields for non-null fields, which
            allows for creation of partial dictionaries for updates """
        p = {}
        if member.get('last_name') is not None:
            p['family_name'] = member.get('last_name')

        if member.get('first_name') is not None:
            p['given_name'] = member.get('first_name')

        if member.get('zip_code') is not None:
            p['postal_addresses'] = [{'postal_code': member.get('zip_code')}]

        if member.get('email_address') is not None:
            p['email_addresses'] = [{'address': member.get('email_address')}]

        return p

    def _sieve_of_inimportance(self, members_json):
        def has_livestream(member):
            if 'livestream 4/17' in member['custom_fields']:
                if member['custom_fields']['livestream 4/17'] == 'yes':
                    return True
            return False
        def is_unsubscribed(member):
            if member['email_addresses'][0]['status'] != 'subscribed':
                return True
            return False

        keep_members = []
        toss_members = []
        for m in members_json:
            if not has_livestream(m) and not is_unsubscribed(m):
                keep_members.append(m)
            else:
                toss_members.append(m)
        return keep_members, toss_members

    def _create_members_from(self, an_json):
        members_json = an_json['_embedded']['osdi:people']
        keep_json, toss_json = self._sieve_of_inimportance(members_json)
        keep_members = [self._json_to_member(x) for x in keep_json]
        toss_members = [self._json_to_member(x) for x in toss_json]
        return keep_members, toss_members

    def create_members(self):
        keeps = []
        tosses = []
        page = 0
        href = self.href
        while True:
            an_json = self.make_request(
                href = href,
                params = self.params,
                headers = self.headers,
                cache_filename = 'cache/an_cache_%s.json' % page)
            keep, toss = self._create_members_from(an_json)
            keeps.extend(keep)
            tosses.extend(toss)

            href = an_json
            if 'next' not in an_json['_links']:
                self.tossed_members = tosses
                return keeps

            href = an_json['_links']['next']['href']
            page += 1
            assert page < 500 # safety check

    def _do_action_create(self, action):
        formatted_member = self._member_to_json(action.member)
        data = self.post_request(href = self.href,
                                 params = self.params,
                                 headers = self.headers,
                                 data = formatted_member)
        return self._json_to_member(data)

    def _do_action_update_helper(self, action, data):
        # Construct URL as per
        # https://actionnetwork.org/docs/v2/queries
        prefix = 'action_network:'
        if prefix not in action.member.unique_id:
            print "Warning! This member has a funny prefix. Not deleting", action.member
            return
        unique_id = action.member.unique_id[len(prefix):]
        href = self.href + "/" + unique_id

        self.put_request(href = href,
                         params = self.params,
                         headers = self.headers,
                         data = data)

    def _do_action_update(self, action):
        data = self._member_to_json(action.member)
        self._do_action_update_helper(action, data)

    def _do_action_delete(self, action):
        # Note: deletion is not allowed, so we update instead
        # https://actionnetwork.org/docs/v2/people#delete
        data = {
          "email_addresses": [
            {
              "status": "unsubscribed"
            }
          ]
        }
        self._do_action_update_helper(action, data)


class ATConnection(Connection):
    fields_to_request = ('Email Address',
                         'First Name',
                         'Last Name',
                         'Zip code')

    def __init__(self, at_token, verbose):
        super(ATConnection, self).__init__(verbose)
        self.headers = {'Authorization': 'Bearer %s' % at_token}
        self.params = {'fields': self.fields_to_request}
        self.href = 'https://api.airtable.com/v0/appKBM2llidtAm4kw/'\
                    'Community%20Members'

    def _json_to_member(self, member_json):
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

    def _member_to_json(self, member):
        """ Allows you to pass in a Member or a dictionary, and
            only returns JSON fields for non-null fields, which
            allows for creation of partial dictionaries for updates """
        p = {}

        conversion = ['email_address', 'first_name', 'last_name', 'zip_code']
        for (ours, theirs) in zip(conversion, self.fields_to_request):
            if member.get(ours) is not None:
                p[theirs] = member.get(ours)

        return {'fields': p}

    def _create_members_from(self, at_json):
        members_json = at_json['records']
        return [self._json_to_member(x) for x in members_json]

    def create_members(self):
        members = []
        page = 0
        params = dict(self.params) # make a copy
        while True:
            at_json = self.make_request(
                              href = self.href,
                              params = params,
                              headers = self.headers,
                              cache_filename = 'cache/at_cache_%s.json' % page)
            members.extend(self._create_members_from(at_json))

            if 'offset' not in at_json:
                return members

            params['offset'] = at_json['offset']
            page += 1
            assert page < 500 # safety check

    def _href_for_member(self, member):
        return self.href + '/' + member.unique_id

    def _do_action_create(self, action):
        formatted_member = self._member_to_json(action.member)
        data = self.post_request(href = self.href,
                                 params = self.params,
                                 headers = self.headers,
                                 data = formatted_member)
        return self._json_to_member(data)

    def _do_action_update(self, action):
        formatted_member = self._member_to_json(action.member)
        href = self._href_for_member(action.member)
        data = self.patch_request(href = href,
                                  params = self.params,
                                  headers = self.headers,
                                  data = formatted_member)

    def _do_action_delete(self, action):
        href = self._href_for_member(action.member)
        data = self.delete_request(href = href,
                                   params = self.params,
                                   headers = self.headers)
