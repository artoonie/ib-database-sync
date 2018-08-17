"""
Prints out conflicts between Indivisible Berkeley's AirTable
and ActionNetwork databases.
"""

import argparse

from connections import ANConnection, ATConnection

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
            print "Invalid selection."
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
