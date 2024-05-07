import requests

class LASTFMAPI:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url

    def fetch_artist_info(self, artist_name):
        params = {
            'method': 'artist.getinfo',
            'artist': artist_name,
            'api_key': self.api_key,
            'format': 'json'
        }
        response = requests.get(self.base_url, params=params)
        return response.json()

    def fetch_album_info(self, album_name, artist_name):
        params = {
            'method': 'album.getinfo',
            'album': album_name,
            'artist': artist_name,
            'api_key': self.api_key,
            'format': 'json'
        }
        response = requests.get(self.base_url, params=params)
        return response.json()