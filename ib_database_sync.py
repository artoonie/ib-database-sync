"""
Prints out conflicts between Indivisible Berkeley's AirTable
and ActionNetwork databases.
"""

import argparse
import json
from tqdm import tqdm

from connections import ANConnection, ATConnection
from records import Member

def msg(s="", end='\n'):
    tqdm.write(s, end=end)

class Prompter(object):
    def __init__(self, title):
        self._title = title
        self._options = []
        self._members = []

    def add_option(self, key, description):
        self._options.append((str(key), description))

    def _add_member_helper(self, member, desc):
        member_i = 1 + len(self._members)
        self.add_option(member_i, desc)
        self._members.append(member)

    def add_next(self):
        self.add_option('n', 'next conflict (skip)')

    def add_quit(self):
        self.add_option('q', 'quit')

    def add_member(self, member):
        desc = "%15s: %s" % (member.source_name, member.prettystring())
        self._add_member_helper(member, desc)

    def add_member_field(self, member, field):
        desc = "%15s: %s = %s" % (member.source_name, field, member.get(field))
        self._add_member_helper(member, desc)

    def prompt(self):
        msg(self._title)
        for key, description in self._options:
            msg("[%s] %s" % (key, description))

        try:
            msg('choose option $> ', end='')
            option = raw_input()
        except KeyboardInterrupt:
            if 'q' in [key for key,_ in self._options]:
                return 'q'
            else:
                raise
        msg()

        try:
            i = int(option)
            if i >= 1 and i <= len(self._members):
                return self._members[i-1]
        except ValueError:
            pass

        if option in [key for key,_ in self._options]:
            return option

        msg("Invalid choice")
        return self.prompt()

class MergeConflict(object):
    def __init__(self, members, resolvers):
        self.members = members
        self.resolvers = resolvers

        for member in self.members:
            assert not member.dirty

    def resolve(self):
        for resolver in self.resolvers:
            if resolver.resolve(self.members, self.members[0].equality_fields):
                return True
        return False

    def get_actions(self):
        actions = []
        for member in self.members:
            if member.dirty:
                actions.append(UpdateAction(member))
        return actions

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
            msg("Automatically resolved conflict: ")
            msg(members[0].prettystring())
            msg()
        return all_conflicts_resolved

class UserQuit(Exception): pass
class ManualResolver(ConflictResolver):
    def prompt_field(self, members, field):
        prompter = Prompter("Which field is correct?")
        for member in members:
            prompter.add_member_field(member, field)
        prompter.add_option('c', 'custom')
        prompter.add_quit()
        return prompter.prompt()

    def prompt(self, members):
        prompter = Prompter("Which member contains correct data?")
        for member in members:
            prompter.add_member(member)
        prompter.add_option('s', 'split into fields')
        prompter.add_next()
        prompter.add_quit()
        return prompter.prompt()

    def set_field_to(self, field, members, selected_member):
        for member in members:
            member.set(field, selected_member.get(field))

    def set_every_field_to(self, equality_fields, members, selected_member):
        for field in equality_fields:
            self.set_field_to(field, members, selected_member)

    def resolve_field(self, members, field):
        option = self.prompt_field(members, field)
        if option == 'q':
            raise UserQuit();
        elif option == 'c':
            user_input = raw_input('enter custom text $> ')
            for member in members:
                member.set(field, user_input)
            return True
        else:
            assert(isinstance(option, Member))
            self.set_field_to(field, members, option)

    def resolve(self, members, equality_fields):
        option = self.prompt(members)

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
            assert(isinstance(option, Member))
            self.set_every_field_to(equality_fields, members, option)
            return True

class Action(object):
    def __init__(self, member):
        self.member = member

    def serialize(self):
        return {'id': self.member.unique_id,
                'action': self.action_name()}

class CreateAction(Action):
    def action_name(self):
        return "create"

class DeleteAction(Action):
    def action_name(self):
        return "delete"

class UpdateAction(Action):
    def action_name(self):
        return "update"

def hash_members(members, equivalence_fields):
    d = {}
    for m in members:
        key = m.hash_with(equivalence_fields)
        if key not in d:
            d[key] = []
        if key in d:
            d[key].append(m)
    return d

def find_duplicates(members, equivalence_fields):
    hm = hash_members(members, equivalence_fields)
    return [hm[key] for key in hm if len(hm[key]) > 1]

def resolve_duplicates(duplicates):
    actions = []
    with tqdm(duplicates) as iterator:
      for curr_duplicates in iterator:
        prompter = Prompter("Which of these duplicates should be kept? "\
                            "(The rest will be deleted)")
        for member in curr_duplicates:
            prompter.add_member(member)
        prompter.add_next()
        prompter.add_quit()
        option = prompter.prompt()
        if option == 'n':
            continue
        elif option == 'q':
            break
        else:
            actions.extend([DeleteAction(m) for m in curr_duplicates if m != option])
    return actions

def print_duplicates_within(dups):
    for dup_list in dups:
        for member in dup_list:
            msg(member.prettystring())
        msg()

def find_duplicates_across(an_members,  at_members, equivalence_fields):
    # Collisions in all_fields means that the two members are exact copies
    an_dict_all_fields  = hash_members(an_members, None)
    at_dict_all_fields  = hash_members(at_members, None)

    # Collisions in some_fields means that the two members share equivalence_fields
    # but may (or may not) share other data.
    an_dict_some_fields = hash_members(an_members, equivalence_fields)
    at_dict_some_fields = hash_members(at_members, equivalence_fields)

    # all_members contains ??
    all_members_keys = set(an_dict_all_fields.keys()+at_dict_all_fields.keys())
    all_members = []
    for key in all_members_keys:
        if key in an_dict_all_fields:
            all_members.append(an_dict_all_fields[key][0])
        else:
            all_members.append(at_dict_all_fields[key][0])


    merge_conflicts = []
    needs_sync = []
    up_to_date = []

    resolvers = [MissingFieldResolver(), ManualResolver()]

    for member in all_members:
        key_in_all = member.hash_with(None)
        key_in_some = member.hash_with(equivalence_fields)

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
    return merge_conflicts, needs_sync, up_to_date

def print_merge_conflicts(merge_conflicts):
    for conflict in merge_conflicts:
        for member in conflict.members:
            name = member.source_name
            msg("%30s has:" % name, member.prettystring())
        msg()

def resolve_merge_conflicts(merge_conflicts):
    actions = []
    with tqdm(merge_conflicts) as iterator:
      for i, merge_conflict in enumerate(iterator):
        try:
            wasResolved = merge_conflict.resolve()
            if wasResolved:
                actions.extend(merge_conflict.get_actions())
        except UserQuit:
            break
    return actions

def print_needs_sync(needs_sync, equivalence_fields,
                     at_dict_some_fields, an_dict_some_fields):
    for member in needs_sync:
        key = member.hash_with(equivalence_fields)
        if key not in at_dict_some_fields:
            msg("     Airtable is missing member", member.prettystring())
        elif key not in an_dict_some_fields:
            msg("ActionNetwork is missing member", member.prettystring())
        else:
            assert False

def serialize_actions(actions, filename):
    d = [action.serialize() for action in actions]
    with open(filename, 'w') as f:
        json.dump(d, f, indent=4)

def get_merge_info(an_members, at_members, equivalence_fields, verbose):
    an_dups = find_duplicates(an_members, equivalence_fields)
    at_dups = find_duplicates(at_members, equivalence_fields)

    msg("There are %d duplicate members to resolve in ActionNetwork")

    merge_conflicts, needs_sync, up_to_date = find_duplicates_across(
                                an_members, at_members, equivalence_fields)

    if verbose:
        msg("#"*100)
        msg("#"*100)

    msg()
    msg("Based on %s, we have found:" % ', '.join(equivalence_fields))
    if verbose:
        msg("#"*100)
    msg("There are %d duplicated members in AirTable" % len(at_dups))
    if verbose:
        print_duplicates_within(an_dups)
        msg("#"*100)
    msg("There are %d duplicated members in ActionNetwork" % len(an_dups))
    if verbose:
        print_duplicates_within(at_dups)
    if verbose:
        msg("#"*100)

    msg("There are %d merge conflicts" % len(merge_conflicts))
    msg()
    msg()

    actions = []
    filename = "test.json"
    try:
        actions.extend(resolve_duplicates(an_dups))
        actions.extend(resolve_duplicates(at_dups))
        actions.extend(resolve_merge_conflicts(merge_conflicts))
    finally:
        serialize_actions(actions, filename)

    msg("There are %d members that can be cleanly synced" % len(needs_sync))
    msg("There are %d members that are fully synced" % len(up_to_date))
    if verbose:
        print_needs_sync(sorted(needs_sync), equivalence_fields,
                         at_dict_some_fields, an_dict_some_fields)
        print_merge_conflicts(sorted(unresolved_conflicts))

    if verbose:
        msg("#"*100)
        msg("#"*100)

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

    msg("Found %d members on ActionNetwork and %d members on AirTable" % \
            (len(an_members), len(at_members)))

    msg("We filtered out %d/%d members from ActionNetwork." % \
            (an_connection.num_members_filtered,
             len(an_members)+an_connection.num_members_filtered))

    """ Each row here defines an equivalence, meaning, a user with the
        same first AND last name, OR the same email, are considered to be the
        same user. Add rows to add additional equivalences."""
    all_equivalence_fields = [['last_name', 'first_name'],
                              ['email_address']]
    for equivalence_fields in all_equivalence_fields:
        get_merge_info(an_members, at_members, equivalence_fields, verbose)
