import pandas as pd
import os

from config_manager import ConfigManager
from lastfm_api import LastFMAPI
from musicbrainz_api import MusicBrainzAPI
from data_storage import DataStorage
from helper import Helper



class LastFMDataExtractor:
    def __init__(self, config_path, schema_path):
        self.config_manager = ConfigManager(config_path, schema_path)
        # schema = self.config_manager.schema
        self.lastfm_api = LastFMAPI(api_key=self.config_manager.API_KEY
                                    , base_url=self.config_manager.BASE_URL
                                    , username=self.config_manager.USERNAME)
        
        self.musicbrainz_api = MusicBrainzAPI()
        self.storage = DataStorage()
        self.helper = Helper()

    def fill_missing_artist_mbid(self, row, artist_mbid_mapping):
        if pd.isna(row['artist_mbid']) or row['artist_mbid'] == '':
            return artist_mbid_mapping.get(row['artist_name'])
        else:
            return row['artist_mbid']

    def treatment_artist_mbid(self, df_lastfm):
        """
        Let's do some treatment on the artist_mbid. In some rows, for artist that already
        have an artist_mbid, the mbid shows as NaN. Also, since I'm not sure that 
        an artist cant have more than 1 artist_mbid, lets map the artist to the mbid
        that appears the most to him to replace the NaN values
        """
        print('Treat artist mbid')
        # Append already extracted data since some artists might still have a 'null' artist_mbid
        df_existing_lastfm_data = pd.DataFrame(columns = self.config_manager.TRACK_DATA_SCHEMA.keys())
        if os.path.exists(self.config_manager.PATH_EXTRACT) and not self.config_manager.NEW_XLSX:
            df_existing_lastfm_data = self.storage.read_excel(path=self.config_manager.PATH_EXTRACT, schema=self.config_manager.TRACK_DATA_SCHEMA)

        # Append new data to bottom of existing csv
        temp_df = pd.concat([df_existing_lastfm_data, df_lastfm], ignore_index=True)
            
        artist_mbid_counts = temp_df.groupby(['artist_name', 'artist_mbid']).size().reset_index(name='count')
        artist_mbid_mapping = artist_mbid_counts.sort_values(by='count', ascending=False).drop_duplicates(subset='artist_name').set_index('artist_name')['artist_mbid']

        # Fix artist_mbid in both new data and existing data
        for df in [df_lastfm, df_existing_lastfm_data]:
            if not df.empty: df['artist_mbid'] = df.apply(lambda row: self.fill_missing_artist_mbid(row, artist_mbid_mapping), axis=1)


        list_artist_mbid = list(set(df_lastfm[df_lastfm['artist_mbid'].notna()]['artist_mbid']))
        # If MusicBrainz artist file exists, check if we already extracted artists info
        # else, fetch data from all artists
        df_existing_mb_artist_info = pd.DataFrame()
        if os.path.exists(self.config_manager.MB_PATH_ARTIST_INFO) and not self.config_manager.NEW_MB_XLSX:
            # df_existing_mb_artist_info = pd.read_excel(MB_PATH_ARTIST_INFO)
            df_existing_mb_artist_info = self.storage.read_excel(path=self.config_manager.MB_PATH_ARTIST_INFO, schema=self.config_manager.MB_ARTIST_SCHEMA)
            mbids_already_extracted = list(set(df_existing_mb_artist_info[df_existing_mb_artist_info['artist_mbid'].notna()]['artist_mbid']))
            
            artists_to_extract = [mbid for mbid in list_artist_mbid if (mbid and mbid not in mbids_already_extracted)]
        else:
            self.config_manager.NEW_MB_XLSX = True
            artists_to_extract = [mbid for mbid in list_artist_mbid if mbid]
        
        mb_artist_data = self.musicbrainz_api.fetch_artist_info_from_musicbrainz(artists_to_extract)

        df_mb_artist = pd.DataFrame(mb_artist_data)
        # Concating new artist info with the existing artist info
        df_mb_artist = pd.concat([df_mb_artist, df_existing_mb_artist_info], ignore_index=True)

        # Output the CSV with artist info from Music Brainz
        self.storage.output_excel(df=df_mb_artist, path=self.config_manager.MB_PATH_ARTIST_INFO, schema=self.config_manager.MB_ARTIST_SCHEMA)

        # Merge new data into existing data (whose artist_mbid has been corrected)
        df_output = pd.concat([df_existing_lastfm_data, df_lastfm])

        return df_output

    def run(self):
        # Fetch track data with duration
        print('Fetching Lastfm data')
        lastfm_data = self.lastfm_api.extract_track_data()

        # Create DataFrame with lastfm data
        df_lastfm = pd.DataFrame(lastfm_data)

        if self.config_manager.GET_EXTRA_INFO:
            # Create helper table with unique combinations of artist & album/artist
            helper_df_artist = df_lastfm[['artist_name']].drop_duplicates()
            helper_df_album_artist = df_lastfm[['artist_name', 'album_name']].drop_duplicates()

            # Skip already extracted info
            # Skip for albums
            existing_helper_album_df = pd.DataFrame()
            existing_helper_album_df_to_merge = pd.DataFrame(columns=helper_df_album_artist.columns)
            if os.path.exists(self.config_manager.PATH_HELPER_ALBUM_INFO):
                existing_helper_album_df = self.storage.read_excel(path=self.config_manager.PATH_HELPER_ALBUM_INFO)
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
            if os.path.exists(self.config_manager.PATH_HELPER_ARTIST_INFO):
                existing_helper_artist_df = self.storage.read_excel(path=self.config_manager.PATH_HELPER_ARTIST_INFO)
                existing_helper_artist_df_to_merge = existing_helper_artist_df[helper_df_artist.columns]

            merged = helper_df_artist.merge(existing_helper_artist_df_to_merge, 
                                                how='left',
                                                indicator=True, 
                                                on=['artist_name'])
            
            helper_df_artist = merged[merged['_merge']=='left_only']
            helper_df_artist.drop(columns='_merge', inplace=True)

            # Create second helper table with album & artist info
            print('Create helper table - album & artist info')
            album_info_list = []
            for _, row in helper_df_album_artist.iterrows():
                artist_name = row['artist_name']
                album_name = row['album_name']
                album_info = self.lastfm_api.fetch_album_info(artist_name, album_name)
                album_info_list += album_info

            df_album_info = pd.DataFrame(album_info_list)

            artist_info_list = []
            for _, row in helper_df_artist.iterrows():
                artist_name = row['artist_name']
                artist_info = self.lastfm_api.fetch_artist_info(artist_name)
                artist_info_list.append(artist_info)

            df_artist_info = pd.DataFrame(artist_info_list)

            # Save the second helper tables
            self.storage.output_excel(df=df_album_info, path=self.config_manager.PATH_HELPER_ALBUM_INFO, append=True)
            self.storage.output_excel(df=df_artist_info, path=self.config_manager.PATH_HELPER_ARTIST_INFO, append=True)
            
            # Add new dataframes to existing dataframes
            df_album_info = pd.concat([existing_helper_album_df, df_album_info])
            df_album_info = df_album_info.sort_values('track_duration', ascending=False).drop_duplicates(['artist_name', 'album_name', 'track_name'])
            
            df_artist_info = pd.concat([existing_helper_artist_df, df_artist_info]).drop_duplicates()    
            
            # Add the helper tables to the main lastfm dataframe
            df_lastfm = df_lastfm.merge(df_album_info, how='left', on=['track_name', 'artist_name', 'album_name'])
            df_lastfm = df_lastfm.merge(df_artist_info, how='left', on=['artist_name'])

        # Fetch and extract user data into Excel
        user_data = self.lastfm_api.fetch_user_data()
        df_user = pd.DataFrame(user_data)
        self.storage.output_excel(df=df_user, path=self.config_manager.PATH_USER_INFO)

        df_output = self.treatment_artist_mbid(df_lastfm)

        self.helper.replace_nan(df=df_output, schema=self.config_manager.TRACK_DATA_SCHEMA)

        # Output the CSV with track data
        print('Save final output')
        self.storage.output_excel(df=df_output, path=self.config_manager.PATH_EXTRACT, schema=self.config_manager.TRACK_DATA_SCHEMA)

if __name__ == "__main__":
    extractor = LastFMDataExtractor('config.json', 'schema.json')
    extractor.run()

