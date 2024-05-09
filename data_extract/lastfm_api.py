import requests
from datetime import datetime

from helper import Helper
from config_manager import ConfigManager

class LASTFMAPI:
    def __init__(self, api_key, base_url, username):
        self.config_manager = ConfigManager()
        self.helper = Helper()

        self.api_key = api_key
        self.base_url = base_url
        self.username = username
        self.unix_latest_track_date = self.config_manager.UNIX_LATEST_TRACK_DATE

    # Function to fetch user data
    def fetch_user_data(self):
        params = {
            'method': 'user.getinfo',
            'user': self.username,
            'api_key': self.api_key,
            'format': 'json'
        }
        response = requests.get(self.base_url, params=params)
        data = response.json()
        user_info = data['user']
        
        user_data = {
            'username': user_info['name']
            , 'user_playcount': user_info['playcount']
            , 'user_artist_count': user_info['artist_count']
            , 'user_album_count': user_info['album_count']
            , 'user_track_count': user_info['track_count']
            , 'user_image': self.helper.get_image_text(user_info.get('image', {}), 'extralarge')
            , 'user_country': user_info['country']        
        }
        
        return [user_data]
        
    # Get the total of pages of 1000 tracks each for the user
    def get_total_pages(self, from_date=None, to_date=None):
        params = {
            'method': 'user.getrecenttracks',
            'user': self.username,
            'api_key': self.api_key,
            'from': from_date,
            'to': to_date,
            'format': 'json',
            'limit': 1000
        }
        response = requests.get(self.base_url, params=params)
        data = response.json()
        total_pages = int(data['recenttracks']['@attr']['totalPages'])
        return total_pages

    def extract_track_data(self, from_date=None
                        , to_date=None
                        , number_pages=None
                        , update_config=True):
        """
        Default is to fetch data since last extract.
        
        """
        if from_date is None:
            from_date = self.unix_latest_track_date

        all_tracks = []
        total_pages = self.get_total_pages(from_date=from_date, to_date=to_date)
        track_number = self.config_manager.SCROBBLE_NUMBER
        
        if from_date:
            from_date_obj = datetime.utcfromtimestamp(int(from_date))
            print('From date :', from_date_obj.strftime('%d %b %Y, %H:%M'))
        else:
            from_date_obj = None
            print('Starting from beggining')
        
        # Define page interval
        page = total_pages
        if not number_pages:
            number_pages = total_pages
        page_goal = (total_pages - number_pages) + 1
        
        while page >= page_goal:
            params = {
                'method': 'user.getrecenttracks',
                'user': self.username,
                'api_key': self.api_key,
                'format': 'json',
                'from': from_date,
                'to': to_date,
                'page': page,
                'extended': 0,
                'limit': 1000  # Maximum size by page
            }
            response = requests.get(self.base_url, params=params)
            data = response.json()
            
            if 'error' in data:
                print("Error:", data['message'])
                break
            
            print('Page ', page,' up until ', page_goal)
            
            # Extract relevant track information
            tracks = data['recenttracks'].get('track', [])
            
            # Exclude "now playing" track if present
            if '@attr' in tracks[0] and tracks[0]['@attr'].get('nowplaying') == 'true':
                tracks = tracks[1:]
            
            page_most_recent_track = tracks[0].get('date', {}).get('#text', '')
            page_most_recent_track_obj = datetime.strptime(page_most_recent_track, '%d %b %Y, %H:%M')
            # Skip page if it has already been extracted
            if from_date_obj and from_date_obj >= page_most_recent_track_obj:
                print('Skipping track. Page most recent as of date :', page_most_recent_track)
                page -= 1
                page_goal = max(page_goal - 1, 1)
                continue
                
            for track in tracks:
                # Check if track is more recent than the latest_track_date
                track_date = track.get('date', {}).get('#text', '')
                track_date_obj = datetime.strptime(track_date, '%d %b %Y, %H:%M')
                if from_date_obj and from_date_obj >= track_date_obj:
                    continue  # Skip if track date is not more recent than latest_track_date
                
                if '@attr' in track and track['@attr'].get('nowplaying') == 'true':
                    continue  # Skip "now playing" tracks
                    
                artist_name = track['artist']['#text']
                album_name = track['album']['#text']
                track_name = track['name']
                track_mbid = track.get('mbid', '')
                
                track_number += 1
                
                track_info = {
                    'scrobble_number': track_number
                    , 'username': self.username
                    , 'track_name': track_name
                    , 'track_mbid': track_mbid
                    , 'date': track_date
                    , 'artist_name': artist_name
                    , 'artist_mbid': track['artist'].get('mbid', '')
                    , 'album_name': album_name
                    , 'album_mbid': track['album'].get('mbid', '')
                }
                
                all_tracks.append(track_info)

            page -= 1

        most_recent_date_track = page_most_recent_track
        most_recent_date_track_obj = page_most_recent_track_obj
        LATEST_TRACK_DATE = self.config_manager.LATEST_TRACK_DATE
        LATEST_TRACK_DATE_obj = datetime.strptime(LATEST_TRACK_DATE, '%d %b %Y, %H:%M')

        if update_config:
            if not LATEST_TRACK_DATE or most_recent_date_track_obj >= LATEST_TRACK_DATE_obj:
                self.config_manager.update_latest_track_date(date=most_recent_date_track, track_number=track_number)


        return all_tracks

    # Function to fetch artist info
    def fetch_artist_info(self, artist_name):
        params = {
            'method': 'artist.getinfo',
            'artist': artist_name,
            'api_key': self.api_key,
            'format': 'json'
        }
        response = requests.get(self.base_url, params=params)
        data = response.json()
        
        # Add artist data
        artist_info = data.get('artist', {})
        artist_data = {
            'artist_name': artist_name
            , 'artist_mbid2': artist_info.get('mbid', '')
            , 'artist_listeners': artist_info.get('stats', {}).get('listeners', 0)
            , 'artist_playcount': artist_info.get('stats', {}).get('playcount', 0)
            # , 'artist_image': get_image_text(artist_info.get('image', {}), 'extralarge')
        }
        
        return artist_data

    # Function to fetch album info for a track and details for each track in the album
    def fetch_album_info(self, album_name, artist_name):
        params = {
            'method': 'album.getinfo',
            'album': album_name,
            'artist': artist_name,
            'api_key': self.api_key,
            'format': 'json'
        }
        response = requests.get(self.base_url, params=params)
        data = response.json()
    
        # Extract album data
        album_info = data.get('album', {})
        album_listeners = album_info.get('listeners', 0)
        album_playcount = album_info.get('playcount', 0)
        
        # Prepare list to hold track details
        tracks_details = []
        
        # Extract tracks data
        tracks = album_info.get('tracks', {}).get('track', [])
        # Check if the song is a single
        if isinstance(tracks, dict) and 'duration' in tracks:
            track_details = {
                'artist_name': artist_name,
                'album_name': album_name,
                'track_name': tracks.get('name', ''),
                'album_listeners': album_listeners,
                'album_playcount': album_playcount,
                'track_duration': int(tracks.get('duration', 0) or 0)
            }
            tracks_details.append(track_details)
        else:
            for track in tracks:
                track_details = {
                    'artist_name': artist_name,
                    'album_name': album_name,
                    'track_name': track.get('name', ''),
                    'album_listeners': album_listeners,
                    'album_playcount': album_playcount,
                    'track_duration': int(track.get('duration', 0) or 0)
                }
                tracks_details.append(track_details)
        
        return tracks_details
    
    # Function to extract track duration from album info
    def get_track_duration(self, album_info):
        tracks = album_info.get('tracks', {}).get('track', [])
        
        if isinstance(tracks, dict) and 'duration' in tracks:
            return int(tracks['duration'] or 0)
        elif isinstance(tracks, list):
            for track in tracks:
                if 'duration' in track:
                    try:
                        return int(track['duration'] or 0)
                    except:
                        print(track)
                        return 0
        return 0
    
