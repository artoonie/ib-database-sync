"""
Prints out conflicts between Indivisible Berkeley's AirTable
and ActionNetwork databases.
"""

import os
import time

from math import sin, cos, sqrt, atan2, radians
import argparse
import hashlib
import json
import requests
import timeago

class Member(object):
    def __init__(self, first_name, last_name, email_address,
                 zip_code, last_edit, source_name, unique_id):
        self.first_name     = first_name
        self.last_name      = last_name
        self.email_address  = email_address
        self.zip_code       = zip_code

        # Everything below this line will not be a part of the equality fields
        self.unique_id = unique_id
        self.last_edit = last_edit
        self.source_name = source_name
        self.dirty = False

    def hash_with(self, only_these_fields=None):
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

class HashFriendlyMember(Member):
    """ This class is sortable and hashable based on equality_fields.
        It will be considered equal to another HashFriendlyMember if all
        equality_fields are equal, ignoring the equality of other fields. """
    def __init__(self, *args, **kwargs):
        super(HashFriendlyMember, self).__init__(*args, **kwargs)

        self.equality_fields = ['first_name', 'last_name', 'email_address', 'zip_code']

    def _clean(cls, s):
        """ prepare a string for comparison: convert to lower case and strip """
        if not isinstance(s, basestring): return s
        return s.lower().strip()

    def _is_eq(self, other, field):
        this = self._clean(self.__dict__[field])
        that = self._clean(other.__dict__[field])
        if this == that: return True
        if this is None or that is None: return False
        return this == that

    def __eq__(self, other):
        return all([self._is_eq(other, field) for field in self.equality_fields])

    def __str__(self):
        d = dict([(key, self._clean(self.__dict__[key])) for key in self.equality_fields])
        return str(d)

    def __lt__(self, other):
        sort_order = ['last_name', 'first_name', 'email_address', 'zip_code']
        equality_fields_sorted = sorted(self.equality_fields,
                                        key=lambda x: sort_order.index(x))
        for field in equality_fields_sorted:
            if self._is_eq(other, field):
                continue
            else:
                return self.__dict__[field] < other.__dict__[field]
        return False

    def __hash__(self):
        return int(hashlib.md5(str(self)).hexdigest(), 16)

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

class MergeConflict(object):
    def __init__(self, members, resolvers):
        self.members = members
        self.resolvers = resolvers

    def resolve(self):
        for resolver in self.resolvers:
            if resolver.resolve(self.members, self.members[0].equality_fields):
                return True
        return False

class ConflictResolver(object):
    def __init__(self): pass

    def resolve(self, conflict): assert False # override this

    def is_any_conflict(self, members, field):
        values = [m.get(field) for m in members]
        return not all([v == values[0] for v in values])

class MissingFieldResolver(ConflictResolver):
    def resolve(self, members, equality_fields):
        all_conflicts_resolved = True
        for field in equality_fields:
            if not self.is_any_conflict(members, field):
                continue

            values = [m.get(field) for m in members]
            is_none  = [v for v in values if v is None]
            not_none = [v for v in values if v is not None]
            if len(not_none) > 1:
                # Conflicting values - cannot resolve simply
                all_conflicts_resolved = False
                continue

            assert len(not_none) == 1
            real_value = not_none[0]

            for member in members:
                member.set(field, real_value)
        if all_conflicts_resolved:
            print "Automatically resolved conflict: "
            print members[0].prettystring()
            print
        return all_conflicts_resolved

class UserQuit(Exception): pass
class ManualResolver(ConflictResolver):
    def prompt_field(self, members, field):
        for i, member in enumerate(members):
            print "[%d] %15s: %s = %s" % (i+1, member.source_name, field, member.get(field))
        print "[c] custom"
        print "[q] quit"
        print

    def prompt(self, members):
        print "How would you like to resolve this conflict?:"
        for i, member in enumerate(members):
            print "[%d] %15s: %s" % (i+1, member.source_name, member.prettystring())
        print "[s] split"
        print "[n] next conflict (skip)"
        print "[q] quit"
        print

    def set_field_to(self, field, members, selected_member_i):
        for member in members:
            member.set(field, members[selected_member_i].get(field))

    def set_every_field_to(self, equality_fields, members, selected_member_i):
        for field in equality_fields:
            self.set_field_to(field, members, selected_member_i)

    def which_member_was_chosen(self, members, option):
        """ Returns int(option) if it's within len(members), else None,
            one-indexed. """
        for i in xrange(len(members)):
            if option == str(i+1):
                return i
        return None

    def resolve_field(self, members, field):
        self.prompt_field(members, field)
        option = raw_input('choose option $> ')
        if option == 'q':
            raise UserQuit();
        elif option == 'c':
            user_input = raw_input('enter custom text $> ')
            for member in members:
                member.set(field, user_input)
            return True

        i = self.which_member_was_chosen(members, option)
        if i is None:
            print "Invalid Check"
            self.resolve_field(members, field)
        else:
            self.set_field_to(field, members, i)

    def resolve(self, members, equality_fields):
        self.prompt(members)
        option = raw_input('choose option $> ')

        if option == 'n':
            return False
        elif option == 'q':
            raise UserQuit()
        elif option == 's':
            for field in equality_fields:
                if self.is_any_conflict(members, field):
                    self.resolve_field(members, field)
            return True
        else:
            i = self.which_member_was_chosen(members, option)
            if i is None:
                print "Invalid choice"
                return self.resolve(members, equality_fields)
            else:
                self.set_every_field_to(equality_fields, members, i)
                return True

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

    resolvers = [MissingFieldResolver(), ManualResolver()]

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
                conflict = MergeConflict(members_conflicted, resolvers)
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
    for i, merge_conflict in enumerate(merge_conflicts):
        try:
            wasResolved = merge_conflict.resolve()
            if not wasResolved:
                unresolved.append(merge_conflict)
        except (UserQuit, KeyboardInterrupt):
            unresolved = unresolved + merge_conflicts[i:]
            break
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
    # Collisions in all_fields means that the two members are exact copies
    an_dict_all_fields  = hash_members(an_members, None)
    at_dict_all_fields  = hash_members(at_members, None)
    # Collisions in some_fields means that the two members share field_set
    # but may (or may not) share other data.
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
    print
    print

    unresolved_conflicts = resolve_merge_conflicts(merge_conflicts)

    print "%d/%d conflicts were unresolved." % (len(unresolved_conflicts), len(merge_conflicts))
    print "There are %d members that can be cleanly synced" % len(needs_sync)
    print "There are %d members that are fully synced" % len(up_to_date)
    if verbose:
        print_needs_sync(sorted(needs_sync), field_set,
                         at_dict_some_fields, an_dict_some_fields)
        print_merge_conflicts(sorted(unresolved_conflicts))

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

    print "We filtered out %d/%d members from ActionNetwork." % \
            (an_connection.num_members_filtered,
             len(an_members)+an_connection.num_members_filtered)

    """ Each row here defines an equivalence, meaning, a user with the
        same first AND last name, OR the same email, are considered to be the
        same user. Add rows to add additional equivalences."""
    equivalence_fields = [['last_name', 'first_name'],
                          ['email_address']]
    for field_set in equivalence_fields:
        get_merge_info(an_members, at_members, field_set, verbose)
