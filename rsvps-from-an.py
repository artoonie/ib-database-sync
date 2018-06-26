from contextlib import contextmanager
import os
import hashlib
import requests
import argparse
import json
import time
import timeago

""" This class is sortable and hashable. Add additional fields as desired. """
class Member(object):
    def __init__(self, first_name, last_name, email_address, zip_code, last_edit, source_name):
        self.first_name = first_name
        self.last_name = last_name
        self.email_address = email_address
        self.zip_code = zip_code

        # Equality Fields are a list of fields of Member for which two
        # objects will be considered if all of these fields are equal.
        # This is supported by sorting, hashing, and comparisons.
        self.equality_fields = self.__dict__.keys()

        # Everything below this line will not be a part of the equality fields
        self.last_edit = last_edit
        self.source_name = source_name
        self.dirty = False

    def hash_with(self, only_these_fields = None):
        """ Gets a unique hash using only_these_fields. If left as the default
            None, uses all available fields. """
        if only_these_fields is None:
            only_these_fields = self.equality_fields
        d = dict([(key, self.__dict__[key]) for key in only_these_fields])
        return str(d)

    def get(self, field):
        assert field in self.equality_fields
        return self.__dict__[field]

    def set(self, field, value):
        assert field in self.equality_fields

        if self.__dict__[field] != value:
            self.dirty = True
            self.__dict__[field] = value

    def prettystring(self):
        return "%30s %60s - %10s" % \
                (unicode(self.first_name) + " " + unicode(self.last_name),
                 "<"+unicode(self.email_address)+">",
                 self.zip_code)

    def _isEq(self, other, field):
        this = self.__dict__[field]
        that = other.__dict__[field]
        if this == that: return True
        if this is None or that is None: return False
        return this.lower() == that.lower()

    def __eq__(self, other):
        return all([self._isEq(other, field) for field in self.equality_fields])

    def __str__(self):
        d = dict([(key, self.__dict__[key]) for key in self.equality_fields])
        return str(d)

    def __lt__(self, other):
        sort_order = ['last_name', 'first_name', 'email_address', 'zip_code']
        equality_fields_sorted = sorted(self.equality_fields,
                                        key=lambda x: sort_order.index(x))
        for field in equality_fields_sorted:
            if self._isEq(other, field):
                continue
            else:
                return self.__dict__[field] < other.__dict__[field]
        return False

    def __hash__(self):
        return int(hashlib.md5(str(self)).hexdigest(), 16)

class Connection(object):
    def __init__(self, verbose):
        self.verbose = verbose

    def make_request(self, cache_filename=None):
        """ set cache_filename to enable caching of this result """
        if cache_filename is None or not os.path.exists(cache_filename):
            if self.verbose:
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

    def _create_member_from(self, member_json):
        address = member_json['postal_addresses'][0]
        return Member(
            email_address = member_json['email_addresses'][0]['address'],
            last_edit     = member_json['modified_date'],
            first_name    = member_json.get('given_name'),
            last_name     = member_json.get('family_name'),
            zip_code      = address.get('postal_code'),
            source_name   = "ActionNetwork")

    def _create_members_from(self, an_json):
        members_json = an_json['_embedded']['osdi:people']
        return [self._create_member_from(x) for x in members_json]

    def create_members(self):
        """ Note: destructive; destroys self.href """
        members = []
        page = 0
        while True:
            an_json = self.make_request('an_cache_%s.json' % page)
            members.extend(self._create_members_from(an_json))

            self.href = an_json
            if 'next' not in an_json['_links']:
                return members

            self.href = an_json['_links']['next']['href']
            page += 1
            assert page < 500 # safety check

class ATConnection(Connection):
    fields_to_request = ('Email Address',
                         'First Name',
                         'Last Name',
                         'Zip code')

    def __init__(self, at_token, verbose):
        super(ATConnection, self).__init__(verbose)
        self.header = {'Authorization': 'Bearer %s' % at_token}
        self.params = {'fields': self.fields_to_request}
        self.href = 'https://api.airtable.com/v0/appKBM2llidtAm4kw/'\
                    'Community%20Members'

    def _create_member_from(self, member_json):
        # TODO: Can we get the modified time instead of created?
        last_edit = member_json.get('createdTime', None)

        return Member(
            email_address = member_json.get(self.fields_to_request[0]),
            first_name    = member_json.get(self.fields_to_request[1]),
            last_name     = member_json.get(self.fields_to_request[2]),
            zip_code      = member_json.get(self.fields_to_request[3]),
            last_edit     = last_edit,
            source_name   = "AirTable")


    def _create_members_from(self, at_json):
        members_json = at_json['records']
        return [self._create_member_from(x['fields']) for x in members_json]

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

class MergeConflict(object):
    def __init__(self, members):
        self.members = members

    def phase0_resolve_empties(self):
        all_conflicts_resolved = True
        for field in self.members[0].equality_fields:
            values = [m.get(field) for m in self.members]
            if all([v == values[0] for v in values]):
                # No conflict
                continue

            is_none  = [v for v in values if v is None]
            not_none = [v for v in values if v is not None]
            if len(not_none) > 1:
                # Conflicting values - cannot resolve simply
                all_conflicts_resolved = False
                continue

            assert len(not_none) == 1
            real_value = not_none[0]

            for member in self.members:
                member.set(field, real_value)
        return all_conflicts_resolved

    def resolve(self):
        return self.phase0_resolve_empties()

def hash_members(members, field_set):
    d = {}
    for m in members:
        key = m.hash_with(field_set)
        if key not in d:
            d[key] = []
        if key in d:
            d[key].append(m)
    return d

def find_duplicates_within(hashed_members):
    hm = hashed_members
    return [hm[key] for key in hm if len(hm[key]) > 1]

def print_duplicates_within(dups):
    for dup_list in dups:
        for member in dup_list:
            print member.prettystring()
        print

def find_duplicates_across(all_members, field_set,
                           an_dict_all_fields,  at_dict_all_fields,
                           an_dict_some_fields, at_dict_some_fields):
    merge_conflicts = []
    needs_sync = []
    up_to_date = []

    for member in all_members:
        key_in_all = member.hash_with(None)
        key_in_some = member.hash_with(field_set)

        is_in_at_all = key_in_all in at_dict_all_fields
        is_in_an_all = key_in_all in an_dict_all_fields
        is_in_at_some = key_in_some in at_dict_some_fields
        is_in_an_some = key_in_some in an_dict_some_fields
        assert is_in_at_all or is_in_an_all
        assert is_in_at_some or is_in_an_some

        if not is_in_at_all or not is_in_an_all:
            if is_in_at_some and is_in_an_some:
                members_conflicted = an_dict_some_fields[key_in_some] +\
                                     at_dict_some_fields[key_in_some]
                conflict = MergeConflict(members_conflicted)
                merge_conflicts.append(conflict)
            elif is_in_at_some and not is_in_an_some:
                needs_sync.append(member)
            elif is_in_an_some and not is_in_at_some:
                needs_sync.append(member)
            else:
                assert False
        else:
            up_to_date.append(member)
    return merge_conflicts, needs_sync, up_to_date

def print_merge_conflicts(merge_conflicts):
    for conflict in merge_conflicts:
        for member in conflict.members:
            name = member.source_name
            print "%30s has:" % name, member.prettystring()
        print

def resolve_merge_conflicts(merge_conflicts):
    unresolved = []
    for merge_conflict in merge_conflicts:
        if not merge_conflict.resolve():
            unresolved.append(merge_conflict)
    return unresolved

def print_needs_sync(needs_sync, field_set,
                     at_dict_some_fields, an_dict_some_fields):
    for member in needs_sync:
        key = member.hash_with(field_set)
        if key not in at_dict_some_fields:
            print "     Airtable is missing member", member.prettystring()
        elif key not in an_dict_some_fields:
            print "ActionNetwork is missing member", member.prettystring()
        else:
            assert False

def get_merge_info(an_members, at_members, equivalence_fields, verbose):
    an_dict_all_fields  = hash_members(an_members, None)
    at_dict_all_fields  = hash_members(at_members, None)
    an_dict_some_fields = hash_members(an_members, field_set)
    at_dict_some_fields = hash_members(at_members, field_set)

    an_dups = find_duplicates_within(an_dict_some_fields)
    at_dups = find_duplicates_within(at_dict_some_fields)

    all_members_keys = set(an_dict_all_fields.keys()+at_dict_all_fields.keys())
    all_members = [an_dict_all_fields[key][0] if key in an_dict_all_fields else
                   at_dict_all_fields[key][0] for key in all_members_keys]

    merge_conflicts, needs_sync, up_to_date = find_duplicates_across(
                                all_members, field_set,
                                an_dict_all_fields, at_dict_all_fields,
                                an_dict_some_fields, at_dict_some_fields)

    unresolved_conflicts = resolve_merge_conflicts(merge_conflicts)

    if verbose:
        print "#"*100
        print "#"*100

    print
    print "Based on %s, we have found:" % ', '.join(field_set)
    if verbose:
        print "#"*100
    print "There are %d duplicated members in AirTable" % len(at_dups)
    if verbose:
        print_duplicates_within(an_dups)
        print "#"*100
    print "There are %d duplicated members in ActionNetwork" % len(an_dups)
    if verbose:
        print_duplicates_within(at_dups)
    if verbose:
        print "#"*100
    print "There are %d merge conflicts" % len(merge_conflicts)
    print " of which %d require manual resolution." % len(unresolved_conflicts)
    print "There are %d members that can be cleanly synced" % len(needs_sync)
    print "There are %d members that are fully synced" % len(up_to_date)
    if verbose:
        print_merge_conflicts(sorted(merge_conflicts))
        print_needs_sync(sorted(needs_sync), field_set,
                         at_dict_some_fields, an_dict_some_fields)

    if verbose:
        print "#"*100
        print "#"*100

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--an-api-key',
                        help = 'API Key for Action Network',
                        required = True)
    parser.add_argument('--at-api-key',
                        help = 'API Key for Airtable',
                        required = True)
    parser.add_argument('--list-problem-members', '-v',
                        help = 'Write out all members that need addressing',
                        action='store_true',
                        required = False,
                        default = False)
    args = parser.parse_args()
    an_token = args.an_api_key
    at_token = args.at_api_key
    verbose = args.list_problem_members

    an_connection = ANConnection(an_token, verbose)
    an_members = an_connection.create_members()

    at_connection = ATConnection(at_token, verbose)
    at_members = at_connection.create_members()

    print "Found %d members on ActionNetwork and %d members on AirTable" % \
            (len(an_members), len(at_members))

    """ Each row here defines an equivalence, meaning, a user with the
        same first AND last name, OR the same email, are considered to be the
        same user. Add rows to add additional equivalences."""
    equivalence_fields = [['last_name', 'first_name'],
                          ['email_address']]
    for field_set in equivalence_fields:
        get_merge_info(an_members, at_members, field_set, verbose)
