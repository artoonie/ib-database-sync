from contextlib import contextmanager
import os
import hashlib
import requests
import argparse
import json
import time
import timeago

class Member(object):
    """ Equality Fields are a list of fields of Member for which two
        objects will be considered if all of these fields are equal.
        This is supported by sorting, hashing, and comparisons. """
    equality_fields = []

    def __init__(self, first_name, last_name, email_address, zip_code):
        self.first_name = first_name
        self.last_name = last_name
        self.email_address = email_address
        self.zip_code = zip_code
        Member.equality_fields = self.__dict__.keys()

    def __str__(self):
        d = dict([(key, self.__dict__[key]) for key in Member.equality_fields])
        return str(d)

    def prettystring(self):
        return "%s %s <%s> - %10s" % (self.first_name,
                                      self.last_name,
                                      self.email_address,
                                      self.zip_code)

    @classmethod
    @contextmanager
    def equality_fields_as(cls, fields):
        """ Use this context-managed function to temporarily change what it
            means for two Members to be equal. Useful for pivoting on
            certain fields. """
        orig_fields = cls.equality_fields
        cls.equality_fields = fields
        yield
        cls.equality_fields = orig_fields

    def __lt__(self, other):
        sort_order = ['last_name', 'first_name', 'email_address', 'zip_code']
        equality_fields_sorted = sorted(self.equality_fields,
                                        key=lambda x: sort_order.index(x))
        for field in equality_fields_sorted:
            if self.isEq(other, field):
                continue
            else:
                return self.__dict__[field] < other.__dict__[field]
        return False

    def isEq(self, other, field):
        this = self.__dict__[field]
        that = other.__dict__[field]
        if this == that: return True
        if this is None or that is None: return False
        return this.lower() == that.lower()

    def __eq__(self, other):
        return all([self.isEq(other, field) for field in self.equality_fields])

    def is_mergeable(self, other):
        """ Update this function with any combination of fields which
            suggest that two entries are equivalent and therefore
            mergeable. """
        required_equal_field_sets = [['last_name', 'first_name'],
                                     ['email_address']]

        # Check to see if any of the field sets are the same
        for field_set in required_equal_field_sets:
            isEqual = True
            for field in field_set:
                if not self.isEq(other, field):
                    isEqual = False
                    break
            if isEqual:
                break

        return isEqual

    def __hash__(self):
        return int(hashlib.md5(str(self)).hexdigest(), 16)

class ANMember(Member):
    def __init__(self, json_from_an):
        address = json_from_an['postal_addresses'][0]
        super(ANMember, self).__init__(
            email_address = json_from_an['email_addresses'][0]['address'],
            first_name    = json_from_an.get('given_name'),
            last_name     = json_from_an.get('family_name'),
            zip_code      = address.get('postal_code'))
        #self.last_edit     = json_from_an['modified_date']

class ATMember(Member):
    requestedFields = ['Email Address',
                       'First Name',
                       'Last Name',
                       'Zip code']

    def __init__(self, json_from_at):
        super(ATMember, self).__init__(
            email_address = json_from_at.get(self.requestedFields[0]),
            first_name    = json_from_at.get(self.requestedFields[1]),
            last_name     = json_from_at.get(self.requestedFields[2]),
            zip_code      = json_from_at.get(self.requestedFields[3]))
        #self.last_edit     = json_from_at.get('createdTime', None) # TODO: Is this modified or created?

class Connection(object):
    def make_request(self, cache_filename=None):
        """ set cache_filename to enable caching of this result """
        if cache_filename is None or not os.path.exists(cache_filename):
            print("Requesting " + self.href)
            response = requests.get(self.href,
                                    params = self.params,
                                    headers = self.header)
            time.sleep(0.2) # Respect AirTable rate limiting rules
            json_response = response.json()
            if cache_filename:
                with open(cache_filename, 'w') as cached_json_fp:
                    json.dump(json_response, cached_json_fp,
                              sort_keys=True, indent=4)
        else:
            timestamp = os.path.getctime(cache_filename)
            ago = timeago.format(timestamp)
            print("Loading %s from cache downloaded %s" % (cache_filename, ago))
            with open(cache_filename, 'r') as cached_json_fp:
                json_response = json.load(cached_json_fp)

        return json_response

class ANConnection(Connection):
    def __init__(self, an_token):
        self.header = {'OSDI-API-Token': an_token}
        self.params = None
        self.href = 'https://actionnetwork.org/api/v2/people'

    def _create_members_from(self, an_json):
        members_json = an_json['_embedded']['osdi:people']
        return [ANMember(x) for x in members_json]

    def create_members(self):
        """ Note: destructive; destroys self.href """
        members = []
        page = 0
        while True:
            an_json = self.make_request('an_cache_%s.json' % page)
            members.extend(self._create_members_from(an_json))

            self.href = an_json
            if 'next' not in an_json['_links']:
                print("Loaded %d members from ActionNetwork" % len(members))
                return members

            self.href = an_json['_links']['next']['href']
            page += 1
            assert page < 500 # safety check

class ATConnection(Connection):
    def __init__(self, at_token):
        self.header = {'Authorization': 'Bearer %s' % at_token}
        self.params = {'fields': ATMember.requestedFields}
        self.href = 'https://api.airtable.com/v0/appKBM2llidtAm4kw/'\
                    'Community%20Members'

    def _create_members_from(self, at_json):
        members_json = at_json['records']
        return [ATMember(x['fields']) for x in members_json]

    def create_members(self):
        """ Note: destructive; destroys self.params """
        members = []
        page = 0
        while True:
            at_json = self.make_request('at_cache_%s.json' % page)
            members.extend(self._create_members_from(at_json))

            if 'offset' not in at_json:
                print("Loaded %d members from AirTable" % len(members))
                return members

            self.params['offset'] = at_json['offset']
            page += 1
            assert page < 500 # safety check

def print_differences(an_set, at_set):
    def get(members, i):
        if i < len(members):
            return members[i].prettystring()
        else:
            return ""

    union = an_set.union(at_set)
    not_in_an = union - an_set
    not_in_at = union - at_set
    not_in_an_sorted = sorted(not_in_an)
    not_in_at_sorted = sorted(not_in_at)

    print len(not_in_at), "entries in ActionNetwork not in AirTable"
    print len(not_in_an), "entries in AirTable not in ActionNetwork"
    print "%80s %80s" % ("Entries missing from ActionNetwork", "Entries missing from AirTable")
    for i in xrange(max(len(not_in_an),len(not_in_at))):
        print ("%80s %80s" % (get(not_in_an_sorted, i), get(not_in_at_sorted, i))).encode('utf-8')

parser = argparse.ArgumentParser()
parser.add_argument('--an-api-key',
                    help = 'API Key for Action Network',
                    required = True)
parser.add_argument('--at-api-key',
                    help = 'API Key for Airtable',
                    required = True)
args = parser.parse_args()
an_token = args.an_api_key
at_token = args.at_api_key

an_connection = ANConnection(an_token)
an_members = an_connection.create_members()

at_connection = ATConnection(at_token)
at_members = at_connection.create_members()

print "Found %d members on ActionNetwork and %d members on AirTable" % (len(an_members), len(at_members))

with Member.equality_fields_as(['zip_code']):
    an_set = set(an_members)
    at_set = set(at_members)
    print_differences(an_set, at_set)
