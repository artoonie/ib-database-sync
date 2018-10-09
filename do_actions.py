import argparse
import pickle

from connections import ANConnection, ATConnection

def do_actions(actions, an_connection, at_connection):
    for action in actions:
        print "Doing action:"
        print "   ", action.serialize()

        if action.member.source_name == "AirTable":
            at_connection.do_action(action)
        else:
            # During testing: have not yet set up a staging environment
            # for actionnetwork, so don't commit to the real data
            continue

        # elif action.member.source_name == "ActionNetwork":
        #     an_connection.do_action(action)
        # else: assert False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--an-api-key',
                        help = 'API Key for Action Network',
                        required = True)
    parser.add_argument('--at-api-key',
                        help = 'API Key for Airtable',
                        required = True)
    parser.add_argument('--actions-filename',
                        help = 'Pickled action filename',
                        required = True)
    args = parser.parse_args()
    an_token = args.an_api_key
    at_token = args.at_api_key
    actions_filename = args.actions_filename

    an_connection = ANConnection(an_token, False)
    at_connection = ATConnection(at_token, False)

    with open(actions_filename, 'r') as f:
        actions = pickle.load(f)

    do_actions(actions, an_connection, at_connection)
