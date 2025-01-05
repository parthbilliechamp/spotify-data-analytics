import pandas as pd
import json


class SpotifyDataProcessor:

    def __init__(self):
        pass

    def load_raw_playlist_data(self, path):
        with open(path, "r") as file:
            data = json.load(file)
        return data

    def get_tracks_data(self, raw_data):
        return [item["track"] for item in raw_data["items"]]

    def retrieve_albums_info(self, track_data):
        albums_list = [
            {
                "album_id": item['album']["id"],
                "album_name": item['album']["name"],
                "album_release_date": item['album']["release_date"]
            }
            for item in track_data
        ]
        return pd.DataFrame(albums_list)
    
    def retrieve_songs_info(self, track_data):
        songs_list = [
            {
                "song_id": item['id'],
                "song_name": item["name"],
                "song_popularity": item['popularity']
            }
            for item in track_data
        ]
        return pd.DataFrame(songs_list)

    def retrieve_artists_info(self, track_data):
        artist_list = [
            {
                "artist_id": artist["id"],
                "artist_name": artist["name"],
            }
            for  item in track_data for artist in item['artists']
        ]
        return pd.DataFrame(artist_list)
