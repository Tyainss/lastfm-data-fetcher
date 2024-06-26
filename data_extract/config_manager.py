import json
import os

from helper import Helper

class ConfigManager:
    def __init__(self, config_path='config.json', schema_path='schema.json'):
        self.config_path = config_path
        self.schema_path = schema_path
        self.config = self.load_json(config_path)
        self.schema = self.load_json(schema_path)

        self.GET_EXTRA_INFO = self.config['GET_EXTRA_INFO']
        self.NEW_XLSX = self.config["NEW_LASTFM_XLSX"]
        self.NEW_MB_XLSX = self.config["NEW_MUSICBRAINZ_XLSX"]

        self.USERNAME = self.config['USER_TO_EXTRACT']
        if self.USERNAME not in self.config['USER_EXTRACT_INFO']:
            self.add_user(self.USERNAME)
        elif self.NEW_XLSX:
            self.reset_config()

        self.LATEST_TRACK_DATE = self.config['USER_EXTRACT_INFO'][self.USERNAME]['latest_track_date']
        self.SCROBBLE_NUMBER = self.config['USER_EXTRACT_INFO'][self.USERNAME]['scrobble_number']

        self.API_KEY = self.config['API_KEY']
        self.BASE_URL = 'http://ws.audioscrobbler.com/2.0/'
        self.PATH_EXTRACT = self.config['path_extracted_file'].replace('{username}', self.USERNAME)
        self.PATH_USER_INFO = self.config['path_user_info_file'].replace('{username}', self.USERNAME)
        self.EXTRACT_FOLDER = self.config['extract_folder']
        self.PATH_HELPER_ALBUM_INFO = self.config['path_helper_album_artist']
        self.PATH_HELPER_ARTIST_INFO = self.config['path_helper_artist']
        self.MB_PATH_ARTIST_INFO = self.config['path_musicbrainz_artist_info']

        self.TRACK_DATA_SCHEMA = self.schema['Scrobble Data']
        self.MB_ARTIST_SCHEMA = self.schema['MusicBrainz Data']

        self.ensure_folder_exists(self.EXTRACT_FOLDER)
        self.UNIX_LATEST_TRACK_DATE = Helper().get_unix_latest_track_date(self.LATEST_TRACK_DATE)

    def ensure_folder_exists(self, folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def load_json(self, path):
        try:
            with open(path) as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: The file {path} does not exist.")
            return {}
        except json.JSONDecodeError:
            print(f"Error: The file {path} is not a valid JSON.")
            return {}

    def add_user(self, username):
        self.config['USER_EXTRACT_INFO'][username] = {
            'latest_track_date': "",
            'scrobble_number': 0
        }
        
        self.save_json(self.config_path, self.config)

    def reset_config(self):
        if self.config["NEW_LASTFM_CSV"]:
            self.config['USER_EXTRACT_INFO'][self.USERNAME]['latest_track_date'] = ""
            self.config['USER_EXTRACT_INFO'][self.USERNAME]['scrobble_number'] = 0
            self.save_json(self.config_path, self.config)

    def save_json(self, path, data):
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=4)
                print("Configuration saved successfully.")
        except Exception as e:
            print(f"Error: Could not save the configuration to {path}. {str(e)}")

    def update_latest_track_date(self, date, track_number):
        self.config['USER_EXTRACT_INFO'][self.USERNAME]['latest_track_date'] = date
        self.config['USER_EXTRACT_INFO'][self.USERNAME]['scrobble_number'] = track_number
        self.save_json(self.config_path, self.config)

