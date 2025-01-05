from data_ingestion import SpotifyDataIngestion 
from data_processing import SpotifyDataProcessor
from datetime import datetime
import pandas as pd

class Runner:
    def __init__(self):
        self.ingestion_output_path = f'data/bronze/daily-top50-playlist/{datetime.now().strftime("%Y%m%d")}.json'

    def __invoke_ingestion(self):
        print("Starting data ingestion process.........") 
        spotify_data_ingestion = SpotifyDataIngestion()
        raw_data = spotify_data_ingestion.fetch_daily_top_50_playist_india()
        result = spotify_data_ingestion.save_data_as_json(raw_data, self.ingestion_output_path)
        if (result):
            print("output written successfully")
        else:
            print("error saving the result data")
        print("Data ingestion process completed successfully.........")

    def __invoke_processing(self):
        print("Starting data processing.........")
        spotify_data_processor = SpotifyDataProcessor()
        raw_data = spotify_data_processor.load_raw_playlist_data(self.ingestion_output_path)
        spotify_track_data = spotify_data_processor.get_tracks_data(raw_data)
        
        artists_df = spotify_data_processor.retrieve_artists_info(track_data=spotify_track_data)
        cleaned_artists_df = self.__get_cleaned_artists_data(artists_df)
        cleaned_artists_df.to_csv('data/silver/artists.csv', index=False)

        albums_df = spotify_data_processor.retrieve_albums_info(track_data=spotify_track_data)
        cleaned_albums_df = self.__get_cleaned_albums_data(albums_df)
        cleaned_albums_df.to_csv('data/silver/albums.csv', index=False)

        songs_df = spotify_data_processor.retrieve_songs_info(track_data=spotify_track_data)
        cleaned_songs_df = self.__get_cleaned_songs_data(songs_df)
        cleaned_songs_df.to_csv('data/silver/songs.csv', index=False)
        
        print("Data processing completed successfully.........")

    def __get_cleaned_artists_data(self, artist_df):
        return artist_df.groupby('artist_id').agg({'artist_name': 'first'}).reset_index()
    
    def __get_cleaned_albums_data(self, album_df):
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        album_df.drop_duplicates(inplace=True)
        return album_df
        
    def __get_cleaned_songs_data(self, song_df):
        song_df.drop_duplicates(inplace=True)
        return song_df

    def run(self):
        self.__invoke_ingestion()
        self.__invoke_processing()


Runner().run()