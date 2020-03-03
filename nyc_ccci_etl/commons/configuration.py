import configparser
import os

config = configparser.ConfigParser()
config.read_file(open( os.environ['PYTHONPATH'] + '/settings.ini'))

def get_app_token():
    return config.get('API','app_token')

def get_database_connection_url():
    return config.get('DATABASE','connection_url')