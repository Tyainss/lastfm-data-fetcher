import pandas as pd
import requests
import json

# Load credentials from JSON file
with open('config.json') as f:
    config = json.load(f)

API_KEY = config['API_KEY']
USERNAME = config['USERNAME']
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'

# Function to fetch user data
def fetch_user_data():
    params = {
        'method': 'user.getinfo',
        'user': USERNAME,
        'api_key': API_KEY,
        'format': 'json'
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data['user']

# Get the total of pages of 200 tracks each for the user
def get_total_pages():
    params = {
        'method': 'user.getrecenttracks',
        'user': USERNAME,
        'api_key': API_KEY,
        'format': 'json',
        'limit': 200  # Fetch only one track to get total number of pages
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    total_pages = int(data['recenttracks']['@attr']['totalPages'])
    return total_pages

# Function to fetch track data with duration
def fetch_track_data_with_duration(fetch_all=False, number_pages=1, direction='forward'):
    all_tracks = []
    total_pages = get_total_pages()
    if fetch_all:
        number_pages = total_pages

    # if direction == forward, start from last page to first
    if direction == 'forward':
        page = total_pages
        page_mover = -1
        page_goal = (total_pages - number_pages) + 1
    else:
        page = 1
        page_mover = 1
        page_goal = min(total_pages, number_pages)

    
    while page <= page_goal:
        params = {
            'method': 'user.getrecenttracks',
            'user': USERNAME,
            'api_key': API_KEY,
            'format': 'json',
            'page': page,
            'limit': 200  # Maximum size by page
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        
        if 'error' in data:
            print("Error:", data['message'])
            break
        
        print('Page ', page,' out of ', total_pages)
        
        # Extract relevant track information
        tracks = data['recenttracks'].get('track', [])
        for track in tracks:
            artist_name = track['artist']['#text']
            album_name = track['album']['#text']
            track_name = track['name']
            track_mbid = track.get('mbid', '')
        
            # Fetch album info to get track duration
            album_data = fetch_album_info(artist_name, album_name)
            artist_data = fetch_artist_info(artist_name)
        
            track_info = {
                'track_name': track_name,
                'track_mbid': track_mbid,
                'date': track.get('date', ''),
                'duration_seconds': album_data.get('duration', 0),
                'artist_name': artist_name,
                'artist_mbid': track['artist'].get('mbid', ''),
                'artist_listeners': artist_data.get('listeners', 0),
                'artist_playcount': artist_data.get('playcount', 0),
                'artist_image': artist_data.get('image', ''),
                'album_name': album_name,
                'album_mbid': track['album'].get('mbid', ''),
                'album_listeners': album_data.get('listeners', 0),
                'album_playcount': album_data.get('playcount', 0)
            }
            
            all_tracks.append(track_info)

            if not fetch_all:
                break
        page += 1
        
    return all_tracks

# Function to fetch album info for a track
def fetch_album_info(artist_name, album_name):
    params = {
        'method': 'album.getinfo',
        'artist': artist_name,
        'album': album_name,
        'api_key': API_KEY,
        'format': 'json'
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    # Add album data
    album_info = data.get('album', {})
    album_data = {
        'listeners': album_info.get('listeners', 0),
        'playcount': album_info.get('playcount', 0),
        'duration': get_track_duration(album_info)
    }
    
    return album_data

# Function to fetch artist info
def fetch_artist_info(artist_name):
    params = {
        'method': 'artist.getinfo',
        'artist': artist_name,
        'api_key': API_KEY,
        'format': 'json'
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    # Add artist data
    artist_info = data.get('artist', {})
    artist_data = {
        'listeners': artist_info.get('stats', {}).get('listeners', 0),
        'playcount': artist_info.get('stats', {}).get('playcount', 0),
        'image': get_image_text(artist_info.get('image', {}), 'extralarge')
    }
    
    return artist_data

# Function to extract track duration from album info
def get_track_duration(album_info):
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

# Function to extract '#text' value from 'image' column based on 'size'
def get_image_text(image_list, size):
    for image in image_list:
        if image['size'] == size:
            return image['#text']
    return None

# Fetch track data with duration
track_data = fetch_track_data_with_duration(fetch_all=True, pages=5)

# Create DataFrame
track_df = pd.DataFrame(track_data)


""" List of things to improve:
    - Artist image is blank - Try to get it from MusicBrainz
    - Try to get more artist information from MusicBrainz - Country etc
    - Find a more optimized/fast way of extracting the data, specially with track duration
        - Maybe paralelism?
Chat GPT:
Reduce API Calls: Currently, you are making multiple API calls for each track to fetch album information and artist information separately. Instead, try to retrieve all necessary information for a track in a single API call if possible.

Batch Processing: Consider fetching track information in batches rather than fetching one track at a time. This can help reduce the overhead of making individual API calls for each track.

Cache Data: If the data does not change frequently, consider caching the results of API calls locally to avoid redundant requests to the Last.fm API.

Asynchronous Requests: Use asynchronous programming techniques or libraries like asyncio to make concurrent requests to the Last.fm API, which can significantly speed up the data retrieval process.
"""
