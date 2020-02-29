import configparser
config = configparser.ConfigParser()
config.read_file(open('./settings.ini'))


def get_app_token():
    return config.get('API','app_token')