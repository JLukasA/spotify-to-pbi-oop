from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import Engine
from urllib.parse import urlparse, quote
import datetime
from typing import Any, Optional
import pandas as pd
from tqdm import tqdm
import requests
import time


class AcousticBrainzETL:
    def __init__(self, db_loc: str, app_name: str, email: str):
        self.db_loc = db_loc
        self.engine = None
        self.app_name = app_name
        self.email = email
        self.user_agent = f"{app_name} ({email})"
        self.headers = HEADERS = {
            'User-Agent': self.user_agent,
            'Accept': 'application/json'
        }

    def get_engine(self):
        if not self.engine or self.engine.closed:
            self.engine = create_engine(self.db_loc)
        return self.engine

    def _authenticate(self) -> None:
        pass

    def get_missing_isrc(self) -> list[str]:
        """ Get ISRCs of songs where data is needed to be fetched. """
        with self.engine.begin() as conn:
            query = text(""" 
                SELECT DISTINCT s.isrc 
                FROM song_data s
                LEFT JOIN acousticbrainz_data a on s.isrc = a.isrc
                LEFT JOIN failed_isrcs f on s.isrc = f.isrc
                LEFT JOIN invalid_mbids m on s.isrc = m.isrc
                WHERE s.isrc IS NOT NULL
                AND f.isrc IS NULL
                AND m.isrc IS NULL
                AND a.isrc IS NULL
                """)
            res = conn.execute(query)
            new_isrc = {row.isrc for row in res.fetchall()}
            return list(new_isrc)

    def isrc_to_mbid(self, isrc_list: list[str]) -> tuple[list[Optional[str]], list[str], dict[str, str]]:

        mbid_list = []
        failed_conversion_list = []
        mbid_to_isrc = {}
        print(f"starting process of fetching Musicbrainz IDs using ISRC. should take approximately {len(isrc_list)} seconds.")
        for isrc in tqdm(isrc_list, desc="parsing ISRCs"):
            url = f"https://musicbrainz.org/ws/2/recording/?query=isrc:{quote(isrc)}&fmt=json"
            while True:
                response = requests.get(url, headers=self.headers, timeout=10)
                time.sleep(1)
                if response.status_code == 200:
                    datafile = response.json()
                    if datafile.get("recordings"):
                        # print(f"Successfully fetched mbid for ISRC {isrc}")
                        mbid = datafile["recordings"][0]["id"]
                        mbid_list.append(mbid)
                        mbid_to_isrc[mbid] = isrc
                    else:
                        # print(f"No mbid data available for irsc {isrc}.")
                        failed_conversion_list.append(isrc)
                    break
                if response.status_code == 429:
                    print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                    time.sleep(1)
                else:
                    print(f"Failed fetching mbid. Status code {response.status_code}.")
                    failed_conversion_list.append(isrc)
                    break

        print(f"Process finished. For {len(isrc_list)} ISRCs, MBIDs were found for {len(mbid_list)}, and the extraction failed for {len(failed_conversion_list)}.")
        return mbid_list, failed_conversion_list, mbid_to_isrc

    def extract(self, mbid_list: list[str]) -> tuple[dict[str, dict], list[str]]:
        ab_data = {}
        invalid_mbids = []
        print("Acousticbrainz data extraction initiated.")
        for mbid in mbid_list:
            if not mbid:
                print("MBID is None, skipping.")
                continue
            url = f"https://acousticbrainz.org/api/v1/{mbid}/high-level"

            while True:
                res = requests.get(url, headers=self.headers, timeout=10)
                if res.status_code == 200:
                    # print(f"Success fetching high-level data for mbid {mbid}.")
                    ab_data[mbid] = res.json()
                    break
                elif res.status_code == 429:
                    # print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                    time.sleep(10)
                elif res.status_code == 404:
                    # print(f"No acoustic data found at {url_high}.")
                    invalid_mbids.append(mbid)
                    break
                else:
                    # print(f"Failed fetching high-level data. Status code {res_high.status_code}")
                    break

        invalid_mbids = list(set(invalid_mbids))
        print(f"Acousticbrainz data extraction finished. Out of {len(mbid_list)} MBIDs, data was found for {len(ab_data)}. {len(invalid_mbids)} invalid MBIDs.")
        return ab_data, invalid_mbids

    def transform(self, raw_data: dict[str, Any], mbids: list[Optional[str]], failed_mbids: list[str], mbid_isrc_mapping: dict[str, str]) -> pd.DataFrame:
        output_data = []

        for mbid in mbids:
            if not mbid:
                continue
            if mbid in failed_mbids:
                continue
            isrc = mbid_isrc_mapping.get(mbid)
            if not isrc:
                print(f"Error with fetching isrc using MBID {mbid}.")
                continue

            ab_data = raw_data.get(mbid, {}).get("highlevel", {})
            features = {
                "isrc": isrc,
                "mbid": mbid,
                "danceability": ab_data.get("danceability", {}).get("value"),
                "instrumentality": ab_data.get("voice_instrumental", {}).get("value"),
                "instrumentality_prob": ab_data.get("voice_instrumental", {}).get("probability"),
                "gender": ab_data.get("gender", {}).get("value"),
                "gender_prob": ab_data.get("gender", {}).get("probability"),
                "timbre": ab_data.get("timbre", {}).get("value"),
                "tonality": ab_data.get("tonal_atonal", {}).get("value")
            }
            output_data.append(features)
        df = pd.DataFrame(output_data)
        return df

    def _initialize_database(self) -> None:
        """ Initialise tables if not already existing. """

        with self.engine.begin() as conn:
            query1 = text("""
                CREATE TABLE IF NOT EXISTS acousticbrainz_data (
                    isrc TEXT PRIMARY KEY NOT NULL,     -- International Standard Recording Code
                    mbid TEXT UNIQUE,                   -- MusicBrainz ID, UUID format
                    danceability TEXT,                  -- danceable/not danceable
                    instrumentality TEXT,               -- instrumental/voice
                    instrumentality_prob REAL,          -- probability of being instrumental/voice
                    gender TEXT,                        -- male/female
                    gender_prob REAL,                   -- probability of male/female
                    timbre TEXT,                        -- bright/dark
                    tonality TEXT                       -- tonal/atonal
                        )
            """)
            conn.execute(query1)
            query2 = text(""" CREATE TABLE IF NOT EXISTS failed_isrcs(
                        isrc TEXT PRIMARY KEY,                             -- International Standard Recording Code
                        last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- timestamp of fetching attempt
                        )
            """)
            conn.execute(query2)
            query3 = text(""" CREATE TABLE IF NOT EXISTS invalid_mbids(
                        mbid TEXT PRIMARY KEY,                             -- Musicbrainz ID
                        isrc TEXT,                                         -- International Standard Recording Code
                        last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- timestamp of fetching attempt
                        )
            """)
            conn.execute(query3)

    def load(self, df: pd.DataFrame, failed_mbids: list[str], failed_isrcs: list[str], mbid_isrc_mapping: dict[str, str]) -> None:
        """Load processed data into database."""

        try:
            with self.engine.begin() as conn:

                # acousticbrainz data
                if df.empty:
                    print("DataFrame is empty, no data to upload.")
                else:
                    query = text(""" SELECT mbid from acousticbrainz_data """)
                    res = conn.execute(query)
                    uploaded_mbids = {row.mbid for row in res}
                    new_df = df[~df['mbid'].isin(uploaded_mbids)]
                    if new_df.empty:
                        print("DataFrame is empty after filtering, no data to upload.")
                    else:
                        new_df.to_sql('acousticbrainz_data', con=conn, index=False, if_exists='append')

                # failed ISRCs
                if failed_isrcs:
                    isrc_df = pd.DataFrame({
                        'isrc': failed_isrcs,
                        'last_attempt': pd.Timestamp.utcnow()
                    })
                    isrc_df.to_sql('failed_isrcs', con=conn, index=False, if_exists='append')
                    print(f"{len(isrc_df)} failed ISRCs uploaded to database.")

                # failed MBIDs
                if failed_mbids:
                    mbid_data = []
                    for mbid in failed_mbids:
                        if mbid in mbid_isrc_mapping:
                            isrc = mbid_isrc_mapping.get(mbid)
                            mbid_data.append({
                                'mbid': mbid,
                                'isrc': isrc,
                                'last_attempt': pd.Timestamp.utcnow()
                            })

                    if mbid_data:
                        mbid_df = pd.DataFrame(mbid_data)
                        mbid_df.to_sql('invalid_mbids', con=conn, index=False, if_exists='append')
                        print(f"{len(mbid_df)} failed MBIDs uploaded to database.")

        except Exception as e:
            print(f"Failed to upload to database. Error: {e}")

    def run(self) -> None:
        """Run the complete ETL pipeline."""
        try:
            self.engine = self.get_engine()
            self._initialize_database()
            isrc = self.get_missing_isrc()
            if not isrc:
                print("No new records to add.")
                return
            mbids, failed_isrcs, mbid_isrc_mapping = self.isrc_to_mbid(isrc)
            raw_data, failed_mbids = self.extract(mbids)
            processed_data = self.transform(raw_data, mbids, failed_mbids, mbid_isrc_mapping)
            self.load(processed_data, failed_mbids, failed_isrcs, mbid_isrc_mapping)
        except Exception as e:
            print(f"ETL pipeline failed: {e}")
            return
        finally:
            self.engine.dispose()


def run(db_loc: str, app_name: str, email: str) -> None:
    etl = AcousticBrainzETL(db_loc=db_loc, app_name=app_name, email=email)
    etl.run()
