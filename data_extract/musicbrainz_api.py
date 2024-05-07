import requests

class MusicBrainzAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self.get_access_token()

    def get_access_token(self):
        token_url = 'https://musicbrainz.org/oauth2/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        response = requests.post(token_url, data=data)
        token_data = response.json()
        return token_data.get('access_token')

    def fetch_artist_info(self, artist_mbid):
        url = f'https://musicbrainz.org/ws/2/artist/{artist_mbid}?inc=aliases+tags+ratings+works+url-rels&fmt=json'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(url, headers=headers)
        return response.json()
