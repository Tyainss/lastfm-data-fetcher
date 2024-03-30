import pandas as pd
import requests
import json
import os
from datetime import datetime

# Load credentials from JSON file
with open('config.json') as f:
# with open('new_config.json') as f:
    config = json.load(f)

API_KEY = config['API_KEY']
USERNAME = config['USERNAME']
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'
PATH_EXTRACT = config['path_extracted_file'].replace('{username}', USERNAME)
LATEST_TRACK_DATE = config['latest_track_date']

if LATEST_TRACK_DATE:
    date_obj = datetime.strptime(LATEST_TRACK_DATE, '%d %b %Y, %H:%M')
    UNIX_LATEST_TRACK_DATE = str(int(date_obj.timestamp()))
else:
    UNIX_LATEST_TRACK_DATE = None

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
def get_total_pages(from_date=None, to_date=None):
    params = {
        'method': 'user.getrecenttracks',
        'user': USERNAME,
        'api_key': API_KEY,
        'from': from_date,
        'to': to_date,
        'format': 'json',
        'limit': 200  # Fetch only one track to get total number of pages
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    total_pages = int(data['recenttracks']['@attr']['totalPages'])
    return total_pages


def extract_track_data(from_date=UNIX_LATEST_TRACK_DATE
                       , to_date=None
                       , number_pages=None
                       , update_config=True):
    """
    Default is to fetch data since last extract.
    
    """
    all_tracks = []
    total_pages = get_total_pages(from_date=from_date, to_date=to_date)
    
    # Define page interval
    page = total_pages
    if not number_pages:
        number_pages = total_pages
    page_goal = (total_pages - number_pages) + 1
    
    while page >= page_goal:
        params = {
            'method': 'user.getrecenttracks',
            'user': USERNAME,
            'api_key': API_KEY,
            'format': 'json',
            'from': from_date,
            'to': to_date,
            'page': page,
            'extended': 0,
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
        
        # Exclude "now playing" track if present
        if '@attr' in tracks[0] and tracks[0]['@attr'].get('nowplaying') == 'true':
            tracks = tracks[1:]
        
        page_most_recent_track = tracks[0].get('date', {}).get('#text', '')
        # Skip page if it has already been extracted
        if from_date and from_date >= page_most_recent_track:
            page -= 1
            page_goal = max(page_goal - 1, 1)
            continue
            
        for track in tracks:
            # Check if track is more recent than the latest_track_date
            track_date = track.get('date', {}).get('#text', '')
            if from_date and from_date >= track_date:
                continue  # Skip if track date is not more recent than latest_track_date
            
            if '@attr' in track and track['@attr'].get('nowplaying') == 'true':
                continue  # Skip "now playing" tracks
                
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
                'date': track_date,
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

        page -= 1

    most_recent_date_track = page_most_recent_track
    print('most_recent_track :',most_recent_date_track)
    if update_config:
        if not LATEST_TRACK_DATE or most_recent_date_track >= LATEST_TRACK_DATE:
            config['latest_track_date'] = most_recent_date_track
    
            # Update json file with the most recent date
            # with open('new_config.json', 'w') as f: # Replace by 'config.json' after
            with open('config.json', 'w') as f:
                json.dump(config, f, indent=4)

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

def output_csv(df, path, new=False):
    # If CSV doesn't exist or we want a new CSV, create new CSV. Else append to existing CSV
    if not os.path.exists(path) or new:
        print('New CSV')
        df.to_csv(path, index=False)
    else:
        print('Adding to existing CSV')
        df.to_csv(path, mode='a', index=False, header=False)
  

NEW_CSV = False

# Fetch track data with duration
track_data = extract_track_data(number_pages=2)

# Create DataFrame
track_df = pd.DataFrame(track_data)

# Output the CSV
output_csv(df=track_df, path=PATH_EXTRACT, new=(NEW_CSV or not LATEST_TRACK_DATE))


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
