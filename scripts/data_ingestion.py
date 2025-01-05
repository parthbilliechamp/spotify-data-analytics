import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import json



class SpotifyDataIngestion:
    def __init__(self):
        self.__playlist = "2sOMIgioNPngXojcOuR4tn"
        self.__auth_manager = SpotifyClientCredentials(client_id="98f4e958933043af93820ae812787cf3", client_secret="eeef576411a54fbeb3e0e1551a6d40b8")
        self.__sp = spotipy.Spotify(auth_manager=self.__auth_manager)

    def fetch_daily_top_50_playist_india(self):
        return self.__sp.playlist_items(self.__playlist, limit=50)

    def save_data_as_json(self, playlist_data, output_path):
        with open(output_path, "w") as json_file:
            json.dump(playlist_data, json_file, indent=2)
        return True

# spotify_data_ingestion = SpotifyDataIngestion()
# raw_data = spotify_data_ingestion.fetch_daily_top_50_playist_india()
# result = spotify_data_ingestion.save_raw_playlist_data_csv()
# if (result):
#     print("output written successfully")
# pd.DataFrame(raw_data)
# records = spotify_data_ingestion.fetch_daily_top_50_playist_india()
# albums_data = spotify_data_ingestion.retrieve_albums_info(records)
# tracks_data = spotify_data_ingestion.retrieve_tracks_info(records)
# artists_data = spotify_data_ingestion.retrieve_artists_info(records)
