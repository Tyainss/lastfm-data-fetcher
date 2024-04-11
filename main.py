import pandas as pd
import requests
import json
import os
from datetime import datetime
import time
import pycountry

# Load credentials from JSON file
with open('config.json') as f:
# with open('new_config.json') as f:
    config = json.load(f)

with open('schema.json') as f:
    schema = json.load(f)


GET_EXTRA_INFO = config['GET_EXTRA_INFO']
NEW_CSV = config["NEW_LASTFM_CSV"]
NEW_MB_CSV = config["NEW_MUSICBRAINZ_CSV"]

if NEW_CSV:
    config['latest_track_date'] = ""
    config['scrobble_number'] = 0
    with open('config.json', 'w') as f:
        print('Resetting config')
        json.dump(config, f, indent=4)

API_KEY = config['API_KEY']
USERNAME = config['USERNAME']
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'
PATH_EXTRACT = config['path_extracted_file'].replace('{username}', USERNAME)
PATH_USER_INFO = config['path_user_info_file'].replace('{username}', USERNAME)
LATEST_TRACK_DATE = config['latest_track_date']
SCROBBLE_NUMBER = config['scrobble_number']

EXTRACT_FOLDER = config['extract_folder']
PATH_HELPER_ALBUM_INFO = config['path_helper_album_artist']
PATH_HELPER_ARTIST_INFO = config['path_helper_artist']

MB_PATH_ARTIST_INFO = config['path_musicbrainz_artist_info']
MB_CLIENT_ID = config['MusicBrainz_Client_ID']
MB_CLIENT_SECRET = config['MusicBrainz_Client_Secret']

TRACK_DATA_SCHEMA = schema['Scrobble Data']
MB_ARTIST_SCHEMA = schema['MusicBrainz Data']

if LATEST_TRACK_DATE:
    LATEST_TRACK_DATE_obj = datetime.strptime(LATEST_TRACK_DATE, '%d %b %Y, %H:%M')
    UNIX_LATEST_TRACK_DATE = str(int(LATEST_TRACK_DATE_obj.timestamp()))
else:
    UNIX_LATEST_TRACK_DATE = None

if not os.path.exists(EXTRACT_FOLDER):
    os.makedirs(EXTRACT_FOLDER)

def get_country_name_from_iso_code(iso_code):
    try:
        country = pycountry.countries.get(alpha_2=iso_code.upper())
        if country:
            return country.name
        else:
            return 'Unknown'
    # 'NoneType' errors are common for artist without Country info
    except AttributeError:
        return 'Unknown'
    except Exception as e:
        print(f'Error: {e}')
        return 'Unknown'

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
    user_info = data['user']
    
    user_data = {
        'username': user_info['name']
        , 'user_playcount': user_info['playcount']
        , 'user_artist_count': user_info['artist_count']
        , 'user_album_count': user_info['album_count']
        , 'user_track_count': user_info['track_count']
        , 'user_image': get_image_text(user_info.get('image', {}), 'extralarge')
        , 'user_country': user_info['country']        
    }
    
    return [user_data]

# Get the total of pages of 200 tracks each for the user
def get_total_pages(from_date=None, to_date=None):
    params = {
        'method': 'user.getrecenttracks',
        'user': USERNAME,
        'api_key': API_KEY,
        'from': from_date,
        'to': to_date,
        'format': 'json',
        'limit': 1000
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
    track_number = SCROBBLE_NUMBER
    
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
            'user': USERNAME,
            'api_key': API_KEY,
            'format': 'json',
            'from': from_date,
            'to': to_date,
            'page': page,
            'extended': 0,
            'limit': 1000  # Maximum size by page
        }
        response = requests.get(BASE_URL, params=params)
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
                , 'username': USERNAME
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
    print('most_recent_track :', most_recent_date_track_obj)
    if update_config:
        if not LATEST_TRACK_DATE or most_recent_date_track_obj >= LATEST_TRACK_DATE_obj:
            config['latest_track_date'] = most_recent_date_track
            config['scrobble_number'] = track_number
    
            # Update json file with the most recent date
            with open('config.json', 'w') as f:
                print('Updated latest extracted date in config')
                json.dump(config, f, indent=4)

    return all_tracks

# Function to fetch album info for a track and details for each track in the album
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
        'artist_name': artist_name
        , 'artist_mbid2': artist_info.get('mbid', '')
        , 'artist_listeners': artist_info.get('stats', {}).get('listeners', 0)
        , 'artist_playcount': artist_info.get('stats', {}).get('playcount', 0)
        , 'artist_image': get_image_text(artist_info.get('image', {}), 'extralarge')
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

# Not working yet :/
def get_access_token_musicbrainz(client_id, client_secret):
    token_url = 'https://musicbrainz.org/oauth2/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post(token_url, data=data)
    token_data = response.json()
    access_token = token_data.get('access_token')
    return access_token

def fetch_artist_info_from_musicbrainz(artist_mbid_list):
    """
    Needs 1 second sleep between every call to not reach API limit
    """
    print('Fetching artist info from MusicBrainz')
    all_artists = []
    for artist_mbid in artist_mbid_list:
        # Sleep 1 second to prevent breaking API limit call per second
        time.sleep(1)
        # MusicBrainz API endpoint for fetching artist information
        url = f'https://musicbrainz.org/ws/2/artist/{artist_mbid}?inc=aliases+tags+ratings+works+url-rels&fmt=json'
        # headers = {
        #     'Authorization': f'Bearer {access_token}'
        # }
        # response = requests.get(url, headers=headers)
        response = requests.get(url)
        data = response.json()
    
        tags = data['tags']
        if tags:
            tags.sort(reverse=True, key=lambda x: x['count'])
            main_genre = tags[0].get('name')
        else:
            main_genre = None
        
        country_1 = data['country']
        country_2 = data.get('area', {}).get('iso-3166-1-codes', [''])[0]
        country_name = get_country_name_from_iso_code(country_2 if country_2 else country_1)
        
        career_begin = data.get('life-span', {}).get('begin')
        career_end = data.get('life-span', {}).get('end')
        career_ended = data.get('life-span', {}).get('ended')
        artist_type = data['type']
        
        # # Extracting artist image URL
        # artist_image_url = None
        # for relation in data.get('relations', []):
        #     if relation['type'] == 'image' and relation['url']['resource']:
        #         artist_image_url = relation['url']['resource']
        #         break
        
        # Data treatment on dates to have them in yyyy-MM-DD format
        if career_begin:
            try:
                career_begin = pd.to_datetime(career_begin).strftime('%Y-%m-%d')
            except:
                career_begin = ''
        if career_end:
            try:
                career_end = pd.to_datetime(career_end).strftime('%Y-%m-%d')
            except:
                career_end = ''
        
        artist_info = {
            'artist_mbid': artist_mbid
            , 'artist_country': country_name
            , 'artist_type': artist_type
            , 'artist_main_genre': main_genre
            , 'artist_career_begin': career_begin
            , 'artist_career_end': career_end
            , 'artist_career_ended': career_ended
            # , 'artist_image_url': artist_image_url  # Added artist image URL
        }
        
        all_artists.append(artist_info)
    
    return all_artists

# Function to extract '#text' value from 'image' column based on 'size'
def get_image_text(image_list, size):
    for image in image_list:
        if image['size'] == size:
            return image['#text']
    return None

def output_csv(df, path, append=False):
    if append:
        print('Adding to existing CSV')
        df.to_csv(path, mode='a', index=False, header=False)
    else:
        print('New CSV')
        df.to_csv(path, index=False)

def output_excel(df, path, schema=None, append=False):
    if schema:
        # Convert DataFrame columns to the specified data types
        for column, dtype in schema.items():
            print('Column :', column, 'dtype :', dtype)
            df[column] = df[column].astype(dtype)
    
    if os.path.exists(path) and append:
        existing_df = pd.read_excel(path)
        df = pd.concat([existing_df, df], ignore_index=True)
    
    df.to_excel(path, index=False)

def convert_to_bool(x):
    if pd.isna(x):
        return pd.NA # Use pandas' NA for missing values in boolean columns
    return bool(x)
    
# Data treatment: Replace nan values based on schema dtype
def replace_nan(df, schema):
    for column, dtype in schema.items():
        if dtype == 'str':
            df[column].fillna('', inplace=True)
        elif dtype == 'int64':
            df[column].fillna(0, inplace=True)

# Fetch track data with duration
lastfm_data = extract_track_data(number_pages=10)

# Create DataFrame with lastfm data
df_lastfm = pd.DataFrame(lastfm_data)

if GET_EXTRA_INFO:
    # Create helper table with unique combinations of artist & album/artist
    helper_df_artist = df_lastfm[['artist_name']].drop_duplicates()
    helper_df_album_artist = df_lastfm[['artist_name', 'album_name']].drop_duplicates()

    # Skip already extracted info
    # Skip for albums
    existing_helper_album_df = pd.DataFrame()
    existing_helper_album_df_to_merge = pd.DataFrame(columns=helper_df_album_artist.columns)
    if os.path.exists(PATH_HELPER_ALBUM_INFO):
        existing_helper_album_df = pd.read_excel(PATH_HELPER_ALBUM_INFO)
        existing_helper_album_df_to_merge = existing_helper_album_df[helper_df_album_artist.columns]
    
    merged = helper_df_album_artist.merge(existing_helper_album_df_to_merge, 
                                          how='left',
                                          indicator=True, 
                                          on=['artist_name', 'album_name'])
    
    helper_df_album_artist = merged[merged['_merge']=='left_only']
    helper_df_album_artist.drop(columns='_merge', inplace=True)
    
    # Skip for artists
    existing_helper_artist_df = pd.DataFrame()
    existing_helper_artist_df_to_merge = pd.DataFrame(columns=helper_df_artist.columns)
    if os.path.exists(PATH_HELPER_ARTIST_INFO):
        existing_helper_artist_df = pd.read_excel(PATH_HELPER_ARTIST_INFO)
        existing_helper_artist_df_to_merge = existing_helper_artist_df[helper_df_artist.columns]

    merged = helper_df_artist.merge(existing_helper_artist_df_to_merge, 
                                          how='left',
                                          indicator=True, 
                                          on=['artist_name'])
    
    helper_df_artist = merged[merged['_merge']=='left_only']
    helper_df_artist.drop(columns='_merge', inplace=True)

    # Create second helper table with album & artist info
    album_info_list = []
    for _, row in helper_df_album_artist.iterrows():
        artist_name = row['artist_name']
        album_name = row['album_name']
        album_info = fetch_album_info(artist_name, album_name)
        album_info_list += album_info

    df_album_info = pd.DataFrame(album_info_list)

    artist_info_list = []
    for _, row in helper_df_artist.iterrows():
        artist_name = row['artist_name']
        artist_info = fetch_artist_info(artist_name)
        artist_info_list.append(artist_info)

    df_artist_info = pd.DataFrame(artist_info_list)

    # Save the second helper tables
    output_excel(df=df_album_info, path=PATH_HELPER_ALBUM_INFO, append=True)
    output_excel(df=df_artist_info, path=PATH_HELPER_ARTIST_INFO, append=True)
    
    # Add new dataframes to existing dataframes
    df_album_info = pd.concat([existing_helper_album_df, df_album_info]).drop_duplicates()
    df_artist_info = pd.concat([existing_helper_artist_df, df_artist_info]).drop_duplicates()    
    
    # Add the helper tables to the main lastfm dataframe
    df_lastfm = df_lastfm.merge(df_album_info, how='left', on=['track_name', 'artist_name', 'album_name'])
    df_lastfm = df_lastfm.merge(df_artist_info, how='left', on=['artist_name'])

# Fetch and extract user data into Excel
user_data = fetch_user_data()
df_user = pd.DataFrame(user_data)
output_excel(df=df_user, path=PATH_USER_INFO)



"""
Let's do some treatment on the artist_mbid. In some rows, for artist that already
have an artist_mbid, the mbid shows as NaN. Also, since I'm not sure that 
an artist cant have more than 1 artist_mbid, lets map the artist to the mbid
that appears the most to him to replace the NaN values
"""
def fill_missing_artist_mbid(row):
    if pd.isna(row['artist_mbid']) or row['artist_mbid'] == '':
        return artist_mbid_mapping.get(row['artist_name'])
    else:
        return row['artist_mbid']
    
# Append already extracted data since some artists might still have a 'null' artist_mbid
df_existing_lastfm_data = pd.DataFrame(columns = TRACK_DATA_SCHEMA.keys())
if os.path.exists(PATH_EXTRACT) and not NEW_CSV:
    df_existing_lastfm_data = pd.read_excel(PATH_EXTRACT)

# Append new data to bottom of existing csv
temp_df = pd.concat([df_existing_lastfm_data, df_lastfm], ignore_index=True)
    
artist_mbid_counts = temp_df.groupby(['artist_name', 'artist_mbid']).size().reset_index(name='count')
artist_mbid_mapping = artist_mbid_counts.sort_values(by='count', ascending=False).drop_duplicates(subset='artist_name').set_index('artist_name')['artist_mbid']

# Fix artist_mbid in both new data and existing data
for df in [df_lastfm, df_existing_lastfm_data]:
    if not df.empty: df['artist_mbid'] = df.apply(fill_missing_artist_mbid, axis=1)


list_artist_mbid = list(set(df_lastfm[df_lastfm['artist_mbid'].notna()]['artist_mbid']))
# If MusicBrainz artist file exists, check if we already extracted artists info
# else, fetch data from all artists
df_existing_mb_artist_info = pd.DataFrame()
if os.path.exists(MB_PATH_ARTIST_INFO) and not NEW_MB_CSV:
    df_existing_mb_artist_info = pd.read_excel(MB_PATH_ARTIST_INFO)
    mbids_already_extracted = list(set(df_existing_mb_artist_info[df_existing_mb_artist_info['artist_mbid'].notna()]['artist_mbid']))
    
    artists_to_extract = [mbid for mbid in list_artist_mbid if (mbid and mbid not in mbids_already_extracted)]
else:
    NEW_MB_CSV = True
    artists_to_extract = [mbid for mbid in list_artist_mbid if mbid]
    
mb_artist_data = fetch_artist_info_from_musicbrainz(artists_to_extract)

df_mb_artist = pd.DataFrame(mb_artist_data)
# Concating new artist info with the existing artist info
df_mb_artist = pd.concat([df_mb_artist, df_existing_mb_artist_info])

# Output the CSV with artist info from Music Brainz
output_excel(df=df_mb_artist, path=MB_PATH_ARTIST_INFO, schema=MB_ARTIST_SCHEMA)


# Merge new data into existing data (whose artist_mbid has been corrected)
df_output = pd.concat([df_existing_lastfm_data, df_lastfm])

# Do some data treatment on the output dataframe
replace_nan(df=df_output, schema=TRACK_DATA_SCHEMA)

# Output the CSV with track data
output_excel(df=df_output, path=PATH_EXTRACT, schema=TRACK_DATA_SCHEMA)


""" List of things to improve:
    - Add validations - Scrobble_number vs # of rows in CSV
    - Format the code better to prevent a long piece of code - with classes | After finishing the code / not requiring high amounts of testing
    - Artist image is blank - Try to get it from MusicBrainz | Tableau seems to not be able to read all images correctly, even after getting them from Wikidata API
        - Not worth the effort for now
    - Find a more optimized/fast way of extracting the data, specially with track duration
        - Maybe paralelism?

Asynchronous Requests: Use asynchronous programming techniques or libraries like asyncio to make concurrent requests to the Last.fm API, which can significantly speed up the data retrieval process.
"""
