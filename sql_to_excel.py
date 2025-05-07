from sqlalchemy import create_engine, text
import pandas as pd
import datetime
from datetime import datetime
import os


class DatabaseToExcelExtraction:
    def __init__(self, db_loc: str):
        self.db_loc = db_loc
        self._engine = None
        self.output_directory = "./data/exports"

    def _get_engine(self):
        if self._engine is None or self._engine.closed:
            self._engine = create_engine(self.db_loc)
        return self._engine

    def _generate_output_path(self, table_name: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.output_directory}/{table_name}_{timestamp}.xlsx"

    def _create_hourly_sheet(self):
        with self._engine.begin() as conn:
            query1 = text("""
            SELECT
                EXTRACT(HOUR FROM p.played_at::timestamp) AS hour_of_day,
                ROUND(AVG(CASE
                            WHEN ab.danceability = 'danceable' THEN 1
                            WHEN ab.danceability = 'not_danceable' THEN 0
                            ELSE 0.5
                                END)::numeric, 2) AS danceability_score,
                ROUND(AVG(CASE
                            WHEN ab.timbre = 'bright' THEN 1
                            WHEN ab.timbre = 'dark' then 0
                            ELSE 0.5
                        END)::numeric, 2) AS brightness_score,
                ROUND(AVG(CASE
                            WHEN ab.gender = 'male' THEN ab.gender_prob
                            WHEN ab.gender = 'female' then -ab.gender_prob
                            ELSE 0
                        END)::numeric, 2) AS male_score,
                COUNT(*) as entries
            FROM plays p
            JOIN song_data s ON p.track_id = s.track_id
            JOIN acousticbrainz_data ab ON s.isrc = ab.isrc
            WHERE ab.danceability IS NOT NULL
            AND ab.timbre IS NOT NULL
            GROUP BY hour_of_day
            ORDER BY hour_of_day
            """)
            df_hourly = pd.read_sql(query1, conn)

            query2 = text("""
            SELECT p.played_at, a.artist_genre
            FROM plays p
            JOIN song_data s ON p.track_id = s.track_id
            JOIN artist_data a ON s.artist_id = a.artist_id
            WHERE a.artist_genre != ''
            """)
            df_genres = pd.read_sql(query2, conn)
            df_genres['artist_genre'] = df_genres['artist_genre'].str.split(',\\s*')
            exploded_df = df_genres.explode('artist_genre')
            exploded_df['hour'] = pd.to_datetime(exploded_df['played_at'], format='ISO8601').dt.hour
            genre_counts = exploded_df.groupby(['hour', 'artist_genre']).size().reset_index(name='count')
            most_common_genres = genre_counts.loc[genre_counts.groupby('hour')['count'].idxmax()]
            most_common_genres = most_common_genres.rename(columns={'artist_genre': 'most_common_genre'})

            df_final = df_hourly.merge(
                most_common_genres[['hour', 'most_common_genre']],
                left_on='hour_of_day',
                right_on='hour',
                how='left'
            )
            df_final = df_final[['hour_of_day', 'brightness_score', 'danceability_score', 'most_common_genre', 'male_score', 'entries']]
            df_final['hour_of_day'] = (df_final['hour_of_day'] + 2) % 24  # from ISO8601 to CET
            table_name = "data_by_hour"
            output_path = self._generate_output_path(table_name)
            df_final.to_excel(output_path, index=False)

    def _create_daily_sheet(self):
        with self._engine.begin() as conn:
            query = query = text("""
                SELECT
                    EXTRACT(DOW FROM p.played_at::timestamp) AS weekday,
                    -- ...
                    COUNT(*) as entries
                FROM plays p
                JOIN song_data s ON p.track_id = s.track_id
                JOIN acousticbrainz_data ab ON s.isrc = ab.isrc
                WHERE ab.danceability IS NOT NULL
                AND ab.timbre IS NOT NULL
                GROUP BY weekday
                ORDER BY weekday
            """)
            df = pd.read_sql
            table_name = "data_by_day"
            output_path = self._generate_output_path(table_name)
            df.to_excel(output_path, index=False)

    def _create_large_sheet(self):
        with self._engine.begin() as conn:
            query = text(""" 
                SELECT  p.played_at, p.track_id,
                        s.song_name, s.featured_artists, s.album_name,
                        s.release_date, s.duration_sec, s.artist_id, s.spotify_url, s.isrc,
                        a.artist_name, a.artist_genre,
                        ab.mbid, ab.danceability, ab.instrumentality, ab.instrumentality_prob, ab.gender,
                        ab.gender_prob, ab.timbre, ab.tonality
                FROM plays p
                JOIN song_data s ON p.track_id = s.track_id
                JOIN artist_data a ON s.artist_id = a.artist_id
                JOIN acousticbrainz_data ab ON s.isrc = ab.isrc
            """)
            df = pd.read_sql(query, conn)
            table_name = "large_sheet"
            output_path = self._generate_output_path(table_name)
            df.to_excel(output_path)

    def run(self):
        """ Run the extraction, gathering data from database tables and creating excel spreadsheets for analysis purposes. """
        try:
            self._engine = self._get_engine()
            os.makedirs(self.output_directory, exist_ok=True)
            self._create_hourly_sheet()
            self._create_large_sheet()
        except Exception as e:
            print(f"Database to excel-extraction failed: {e}")
            return None
        finally:
            if self._engine is not None:
                self._engine.dispose()


def run(db_loc: str) -> None:
    extraction = DatabaseToExcelExtraction(db_loc=db_loc)
    extraction.run()
