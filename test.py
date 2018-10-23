import os
import nose

from contextlib import contextmanager

from connections import ANConnection, ATConnection
from actions import CreateAction, DeleteAction, UpdateAction
from records import Member
from ib_database_sync import ZipCodeResolver

@contextmanager
def assert_raises(exception_type):
    try:
        yield
    except exception_type:
        return

    assert False

def test_connections():
    an_token = os.environ.get('AN_API_KEY')
    at_token = os.environ.get('AT_API_KEY')
    if an_token is None or at_token is None:
        raise ValueError("Environment variables AT_API_KEY and AN_API_KEY "\
                         "must be set.")
    an_connection = ANConnection(an_token, verbose=False)
    at_connection = ATConnection(at_token, verbose=False)

    _test_connection_with(at_connection)
    _test_connection_with(an_connection)

def test_zip_resolver():
    resolver = ZipCodeResolver()

    member0 = _get_fake_member()
    member1 = _get_fake_member()
    member0.zip_code = "12345"
    member1.zip_code = "12345-6789"
    member0._dirty = False
    member1._dirty = False
    equality_fields = ['first_name', 'last_name', 'email_address', 'zip_code']

    member1.first_name = "different name"
    member1._dirty = False
    assert not resolver.resolve([member0, member1], equality_fields)
    assert not member0.dirty
    assert not member1.dirty
    member1.first_name = member0.first_name
    member1._dirty = False

    assert resolver.resolve([member0, member1], equality_fields)
    assert member0.zip_code == "12345-6789"
    assert member1.zip_code == "12345-6789"

    member0.zip_code = "an_email@gmail.com"
    member0._dirty = False
    assert resolver.resolve([member0, member1], equality_fields)
    assert member0.dirty
    assert not member1.dirty

    member0.zip_code = "00000-0000"
    assert resolver.resolve([member0, member1], equality_fields)

    member0.zip_code = None
    assert resolver.resolve([member0, member1], equality_fields)

def _get_fake_member():
    return Member(
            email_address = "nonexistent@gmail.com",
            last_edit     = 0,
            first_name    = "Armin's",
            last_name     = "Test",
            zip_code      = "94704",
            unique_id     = "action_network:N/A",
            source_name   = "AirTable")

def _test_connection_with(connection):
    # Note: unique_ids start with action_network because that's what's
    # required by AN and AT is agnostic
    test_member = _get_fake_member()
    action = CreateAction(test_member)
    new_member = connection.do_action(action)

    # Modify the created member, especially the email address,
    for member in (test_member, new_member):
        member.set('email_address', "new-email-address@gmail.com")
        member.set('last_name', "new last name")

    # Can't use test_member for UpdateAction since it has an invalid unique_id
    action = UpdateAction(test_member)
    with assert_raises(RuntimeError):
        connection.do_action(action)

    # Instead, you must use the member returned by the do_action
    action = UpdateAction(new_member)
    connection.do_action(action)

    action = DeleteAction(new_member)
    connection.do_action(action)

    nonexistent_member = Member(
            email_address = "nonexistent@example.com",
            last_edit     = 0,
            first_name    = "I don't",
            last_name     = "Exist!",
            zip_code      = "12345",
            unique_id     = "action_network:N/A",
            source_name   = "AirTable")

    action = DeleteAction(nonexistent_member)
    with assert_raises(RuntimeError):
        connection.do_action(action)
