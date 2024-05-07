from config_manager import ConfigManager
from lastfm_api import LastFMAPI
from musicbrainz_api import MusicBrainzAPI
from data_storage import DataStorage


class LastFMDataExtractor:
    def __init__(self, config_path):
        config = config_path.get_config_value
        self.lastfm_api = LastFMAPI(api_key=config("API_KEY"), base_url=config("BASE_URL"))
        self.musicbrainz_api = MusicBrainzAPI(client_id=config("MusicBrainz_Client_ID"), client_secret=config("MusicBrainz_Client_Secret"))
        self.storage = DataStorage(path=config("path_musicbrainz_artist_info"))

    def extract_and_save_data(self, artist_names):
        artist_info = [self.lastfm_api.fetch_artist_info(name) for name in artist_names]
        self.storage.save_data(artist_info, schema='LastFM Artist Info')

    def update_musicbrainz_data(self, artist_mbids):
        mb_artist_info = [self.musicbrainz_api.fetch_artist_info(mbid) for mbid in artist_mbids]
        self.storage.save_data(mb_artist_info, schema='MusicBrainz Artist Info')

if __name__ == "__main__":
    extractor = LastFMDataExtractor('config.json')
    extractor.run()