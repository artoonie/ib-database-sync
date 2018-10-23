"""
A common interface for a Member record,
along with helpers to make it hashable based on a subset of the fields
"""

class Member(object):
    def __init__(self, first_name, last_name, email_address,
                 zip_code, last_edit, source_name, unique_id):
        # Member data
        self._first_name     = first_name
        self._last_name      = last_name
        self._email_address  = email_address
        self._zip_code       = zip_code

        # Metadata
        self._unique_id = unique_id
        self._last_edit = last_edit
        self._source_name = source_name
        self._dirty = False
        self._original_dict = {} # what's dirty?

    @property
    def first_name(self):
        return self._first_name

    @first_name.setter
    def first_name(self, value):
        return self.set('first_name', value)

    @property
    def last_name(self):
        return self._last_name

    @last_name.setter
    def last_name(self, value):
        return self.set('last_name', value)

    @property
    def email_address(self):
        return self._email_address

    @email_address.setter
    def email_address(self, value):
        return self.set('email_address', value)

    @property
    def zip_code(self):
        return self._zip_code

    @zip_code.setter
    def zip_code(self, value):
        self.set('zip_code', value)

    @property
    def unique_id(self):
        return self._unique_id

    @unique_id.setter
    def unique_id(self, value):
        return self.set('unique_id', value)

    @property
    def last_edit(self):
        return self._last_edit

    @last_edit.setter
    def last_edit(self, value):
        return self.set('last_edit', value)

    @property
    def source_name(self):
        return self._source_name

    @source_name.setter
    def source_name(self, value):
        return self.set('source_name', value)

    @property
    def dirty(self):
        return self._dirty

    def get(self, field):
        return self.__dict__["_"+field]

    def set(self, field, new_value):
        curr_value = self.get(field)
        if curr_value != new_value:
            self._dirty = True
            self._original_dict[field] = curr_value
            self.__dict__["_"+field] = new_value

    def prettystring(self):
        return "%30s %60s - %10s" % \
                (unicode(self.first_name) + " " + unicode(self.last_name),
                 "<"+unicode(self.email_address)+">",
                 self.zip_code)

    def dirty_fields(self):
        return self._original_dict.keys()

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
        cleaned = s.lower().strip()
        if cleaned == "": return None
        return cleaned

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
