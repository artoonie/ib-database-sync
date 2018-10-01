from setuptools import setup, find_packages

setup(
    name='ib_database_sync',
    app=['ib_database_sync.py'],

    version="0.0.1",
    packages=find_packages(),
    include_package_data=True,
    url='https://github.com/artoonie/ib-database-sync',
    install_requires = ['nose',
                        'requests',
                        'timeago',
                        'tqdm']
)
