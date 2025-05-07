from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import Engine
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import webbrowser
from urllib.parse import urlparse
import datetime
from typing import Any, Optional
import pandas as pd
import localserver


class SpotifyETL:
    def __init__(self, db_loc: str, client_id: str, client_secret: str, redirect_uri: str):

        self.db_loc = db_loc
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.sp_client = None
        self.engine = None
        self.token_cache_path = "./config/spotify_token_cache.json"

    def _get_engine(self):
        if not self.engine or self.engine.closed:
            self.engine = create_engine(self.db_loc)
        return self.engine

    def _authenticate(self) -> spotipy.Spotify:
        auth_manager = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope="user-read-recently-played",
            cache_path=self.token_cache_path
        )

        token_info = auth_manager.get_cached_token()
        if not token_info:
            auth_url = auth_manager.get_authorize_url()
            webbrowser.open(auth_url)
            parsed_uri = urlparse(self.redirect_uri)
            server_address = (parsed_uri.hostname, parsed_uri.port)
            code = localserver.run_server(server_address)
            if code:
                token_info = auth_manager.get_access_token(code)

        return spotipy.Spotify(auth_manager=auth_manager)

    def _get_spotify_client(self) -> spotipy.Spotify:
        if self.sp_client is None:
            self.sp_client = self._authenticate()
        return self.sp_client

    def _extract(self) -> dict[str, Any]:
        sp = self._get_spotify_client()
        today = datetime.datetime.now(datetime.timezone.utc)
        yesterday_unix = int((today - datetime.timedelta(days=1)).timestamp() * 1000)
        return sp.current_user_recently_played(limit=50, after=yesterday_unix)

    def _initialize_database(self) -> None:
        """ Initialise tables if not already existing. """

        with self.engine.begin() as conn:
            query1 = text("""
                CREATE TABLE IF NOT EXISTS plays (       
                    played_at TEXT PRIMARY KEY,
                    track_id TEXT
                )
            """)
            conn.execute(query1)
            query2 = text(""" 
                CREATE TABLE IF NOT EXISTS song_data (       
                    track_id TEXT PRIMARY KEY,
                    song_name TEXT,
                    featured_artists TEXT,
                    album_name TEXT,
                    release_date TEXT,
                    duration_sec INTEGER,
                    artist_id TEXT,
                    spotify_url TEXT,
                    isrc TEXT
                )
            """)
            conn.execute(query2)
            query3 = text(""" 
                CREATE TABLE IF NOT EXISTS artist_data (       
                    artist_id TEXT PRIMARY KEY,
                    artist_name TEXT,
                    artist_genre TEXT
                ) 
            """)
            conn.execute(query3)
            query4 = text(""" 
                CREATE TABLE IF NOT EXISTS genres (
                    artist_id TEXT,
                    genre TEXT,
                    PRIMARY KEY (artist_id, genre),
                    FOREIGN KEY (artist_id) REFERENCES artist_data (artist_id)
                ) 
            """)
            conn.execute(query4)

    def _transform(self, raw_data: dict[str, Any]) -> pd.DataFrame:
        sp = self._get_spotify_client()

        song_name_list, artist_name_list, featured_artist_list = [], [], []
        genre_list, album_name_list = [], []
        duration_list, release_date_list, played_at_list = [], [], []
        spotify_url_list, track_id_list, isrc_list = [], [], []
        artist_id_list = []
        missing_ids = []

        for idx, song in enumerate(raw_data['items']):
            track = song.get('track', {})
            track_id = track.get('id')
            if not track_id:  # Skip if track_id is missing
                missing_ids.append(idx)
                print(f"NO TRACK ID FOR SONG {song}!!! WARNING WARNING WARNING")
                continue
            track_id_list.append(track_id)
            played_at_list.append(song.get("played_at"))
            song_name_list.append(track.get("name"))
            album_name_list.append(track.get("album", {}).get("name"))
            duration_list.append(round(track.get("duration_ms", 0) / 1000))
            release_date_list.append(track.get("album", {}).get("release_date"))
            spotify_url_list.append(track.get("external_urls", {}).get("spotify"))
            isrc_list.append(track.get("external_ids", {}).get("isrc"))
            artists = track.get("artists", [])
            artist_names = [artist.get("name") for artist in artists]
            artist_name_list.append(artist_names[0] if artist_names else "")
            featured_artist_list.append(", ".join(artist_names[1:]) if len(artist_names) > 1 else "")
            artist_id_list.append(artists[0].get("id") if artists else "")

        unique_artist_ids = [id for id in set(artist_id_list) if id]
        try:
            artist_info_list = sp.artists(unique_artist_ids)["artists"]
            artist_info_dict = {artist["id"]: artist for artist in artist_info_list}
        except spotipy.exceptions.SpotifyException as e:
            print(f"Error fetching artist information: {e}")
            artist_info_dict = {}

        genres_dict = {}
        for id in artist_id_list:
            artist_info = artist_info_dict.get(id)
            genres = artist_info.get("genres", [])
            genres_dict[id] = genres

        df = pd.DataFrame({
            "played_at": played_at_list,
            "song_name": song_name_list,
            "artist_name": artist_name_list,
            "featured_artists": featured_artist_list,
            "album_name": album_name_list,
            "release_date": release_date_list,
            "duration_sec": duration_list,
            "track_id": track_id_list,
            "artist_id": artist_id_list,
            "spotify_url": spotify_url_list,
            "isrc": isrc_list
        })
        genres_df = pd.DataFrame([(artist_id, genre) for artist_id, genres in genres_dict.items() for genre in genres],
                                 columns=['artist_id', 'genre'])
        return df, genres_df

    def _validate_data(self, df: pd.DataFrame) -> bool:
        """ Quick data validation before uploading to database. """
        if df.empty:
            print("DataFrame is empty, no songs were downloaded.")
            return False

        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception("Primary key is not unique")

        return True

    def _load(self, df: pd.DataFrame, genres_df: pd.DataFrame) -> None:
        """Load processed data into database."""
        if df.empty:
            print("DataFrame is empty, no data to upload.")
            return
        try:
            with self.engine.begin() as conn:

                # queries used in filtering dataframe before uploading
                query_plays = text(""" 
                    SELECT played_at 
                    FROM plays 
                    ORDER BY played_at DESC
                    LIMIT 1
                """)
                query_songs = text(""" 
                    SELECT track_id 
                    FROM song_data
                """)
                query_artists = text(""" 
                    SELECT artist_id 
                    FROM artist_data
                """)
                query_genres = text(""" 
                    SELECT artist_id, genre 
                    FROM genres
                """)
                # Filter on timestamp, in ISO8601 so converting to datetime for comparison
                latest_uploaded_timestamp = conn.execute(query_plays).scalar()
                df['datetime'] = pd.to_datetime(df['played_at'], format="ISO8601")
                if latest_uploaded_timestamp:
                    latest_uploaded_timestamp_dt = pd.to_datetime(latest_uploaded_timestamp, format="ISO8601")
                    new_df = df[df['datetime'] > latest_uploaded_timestamp_dt]
                else:
                    new_df = df
                new_df = new_df.sort_values(by="datetime", ascending=True)

                if not self._validate_data(new_df):
                    print("Data did not pass validation when uploading plays.")
                    return
                new_df[['played_at', 'track_id']].to_sql('plays', self.engine, index=False, if_exists='append')

                number_of_plays = len(new_df)
                # Filter songs
                res = conn.execute(query_songs)
                uploaded_track_ids = {row.track_id for row in res}
                new_df = new_df[~new_df['track_id'].isin(uploaded_track_ids)]
                new_df = new_df.drop_duplicates(subset='track_id', keep='first')
                new_df[['track_id', 'song_name', 'featured_artists',
                        'album_name', 'release_date', 'duration_sec',
                        'artist_id', 'spotify_url', 'isrc']].to_sql('song_data', self.engine, index=False, if_exists='append')
                number_of_new_songs = len(new_df)
                # Filter artists
                res = conn.execute(query_artists)
                uploaded_artist_ids = {row.artist_id for row in res}
                new_df = new_df[~new_df['artist_id'].isin(uploaded_artist_ids)]
                new_df = new_df.drop_duplicates(subset='artist_id', keep='first')
                new_df[['artist_id', 'artist_name']].to_sql('artist_data', self.engine, index=False, if_exists='append')
                number_of_new_artists = len(new_df)

                # Filter genres
                existing_genres = pd.read_sql(query_genres, conn)
                filtered_genres_df = genres_df[~genres_df.apply(tuple, 1).isin(existing_genres.apply(tuple, 1))]
                filtered_genres_df.to_sql('genres', self.engine, index=False, if_exists='append')
                number_of_genres = len(filtered_genres_df)

                print(
                    f"Data loaded successfully for {number_of_plays} plays. Played {number_of_new_songs} new songs and listened to {number_of_new_artists} new artists. Added {number_of_genres} new genres.")

        except Exception as e:
            print(f"Failed to upload to database. Error: {e}")

    def run(self) -> None:
        """Run the complete ETL pipeline."""
        try:
            self.engine = self._get_engine()
            self._initialize_database()
            raw_data = self._extract()
            processed_data, genres_dict = self._transform(raw_data)
            self._load(processed_data, genres_dict)
            # return processed_data
        except Exception as e:
            print(f"ETL pipeline failed: {e}")
            return None
        finally:
            self.engine.dispose()


def run(db_loc: str, client_id: str, client_secret: str, redirect_uri: str) -> None:
    etl = SpotifyETL(db_loc=db_loc, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)
    etl.run()
