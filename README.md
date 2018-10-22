A way to sync two disparate databases.
The code is built to sync users between ActionNetwork and AirTable databases which may not be in sync.

You can modify it with minimal effort to fit your needs:
1. Extend records.py to contain the records you wish to grab;
2. Extend the Connection object in connections.py to download and upload changes to your custom database.
3 You can choose the pivot keys (a set of unique keys to consider a record in one database the same as a record in another) by adjusting `all_equivalence_fields` in ib_database_sync.py.

Run install.sh to install and run the tests. You must set the environment variables `AN_API_KEY` and `AT_API_KEY` to connect to each database.
