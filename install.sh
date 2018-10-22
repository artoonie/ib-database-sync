virtualenv venv
source venv/bin/activate
pip install -e .
nosetests test.py
