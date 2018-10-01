import os

from connections import ANConnection
from actions import CreateAction, DeleteAction
from records import HashFriendlyMember

def test_create_action():
    an_token = os.environ.get('AN_API_KEY')
    if an_token is None:
        raise ValueError("Environment variable AN_API_KEY must be set.")
    an_connection = ANConnection(an_token, verbose=False)
    test_member = HashFriendlyMember(
            email_address = "zooey.catika@gmail.comm",
            last_edit     = 0,
            first_name    = "Armin's",
            last_name     = "Test",
            zip_code      = "94704",
            unique_id     = "N/A",
            source_name   = "AirTable")

    action = CreateAction(test_member)
    an_connection.do_action(action)

    action = DeleteAction(test_member)
    an_connection.do_action(action)
