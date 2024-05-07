import json

class ConfigManager:
    def __init__(self, config_path='config.json', schema_path='schema.json'):
        self.config_path = config_path
        self.schema_path = schema_path
        self.config = self.load_json(config_path)
        self.schema = self.load_json(schema_path)

        self.API_KEY = self.config['API_KEY']
        self.USERNAME = self.config['USERNAME']
        self.BASE_URL = 'http://ws.audioscrobbler.com/2.0/'
        self.PATH_EXTRACT = self.config['path_extracted_file'].replace('{username}', self.USERNAME)
        self.PATH_USER_INFO = self.config['path_user_info_file'].replace('{username}', self.USERNAME)
        self.LATEST_TRACK_DATE = self.config['latest_track_date']
        self.SCROBBLE_NUMBER = self.config['scrobble_number']
        self.EXTRACT_FOLDER = self.config['extract_folder']
        self.PATH_HELPER_ALBUM_INFO = self.config['path_helper_album_artist']
        self.PATH_HELPER_ARTIST_INFO = self.config['path_helper_artist']
        self.MB_PATH_ARTIST_INFO = self.config['path_musicbrainz_artist_info']
        self.MB_CLIENT_ID = self.config['MusicBrainz_Client_ID']
        self.MB_CLIENT_SECRET = self.config['MusicBrainz_Client_Secret']
        
        self.TRACK_DATA_SCHEMA = self.schema['Scrobble Data']
        self.MB_ARTIST_SCHEMA = self.schema['MusicBrainz Data']

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

    def reset_config(self):
        if self.config["NEW_LASTFM_CSV"]:
            self.config['latest_track_date'] = ""
            self.config['scrobble_number'] = 0
            self.save_json(self.config_path, self.config)

    def save_json(self, path, data):
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=4)
                print("Configuration saved successfully.")
        except Exception as e:
            print(f"Error: Could not save the configuration to {path}. {str(e)}")

    def update_latest_track_date(self, date, track_number):
        self.config['latest_track_date'] = date
        self.config['scrobble_number'] = track_number
        self.save_json(self.config_path, self.config)
