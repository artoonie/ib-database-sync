"""
A common interface for a Member record,
along with helpers to make it hashable based on a subset of the fields
"""

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

    def hash_with(self, only_these_fields=None):
        """ Gets a unique hash using only_these_fields. If left as the default
            None, uses all available fields. """
        if only_these_fields is None:
            only_these_fields = self.equality_fields
        d = dict([(field, self.get_clean(field)) for field in only_these_fields])
        return str(d)

    def get_clean(self, field):
        """ prepare a string for comparison: convert to lower case and strip """
        s = self.get(field)
        if not isinstance(s, basestring): return s
        return s.lower().strip()

    def _is_eq(self, other, field):
        this = self.get_clean(field)
        that = other.get_clean(field)
        if this == that: return True
        if this is None or that is None: return False
        return this == that

    def __eq__(self, other):
        if not isinstance(other, HashFriendlyMember):
            return False
        return all([self._is_eq(other, field) for field in self.equality_fields])

    def __str__(self):
        d = dict([(field, self.get_clean(field)) for field in self.equality_fields])
        return str(d)

    def __lt__(self, other):
        sort_order = ['last_name', 'first_name', 'email_address', 'zip_code']
        equality_fields_sorted = sorted(self.equality_fields,
                                        key=lambda x: sort_order.index(x))
        for field in equality_fields_sorted:
            if self._is_eq(other, field):
                continue
            else:
                return self.get_clean(field) < self.get_clean(field)
        return False

    def __hash__(self):
        return int(hashlib.md5(str(self)).hexdigest(), 16)
