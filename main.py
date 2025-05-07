import spotify_etl
import ab_etl
import sql_to_excel
import visualizer
import json


# DATABASE_LOCATION = "sqlite:///data/my_tracks.sqlite"


# import os
# db_user = os.getenv("DB_USER")
# db_password = os.getenv("DB_PASSWORD")


def load_db_config(config_path='pg_config.json'):
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            required_keys = ["db_user", "db_password", "db_host", "db_port", "db_name"]
            if not all(key in config for key in required_keys):
                raise ValueError(f"Config file must contain: {required_keys}")
            db_user = config['db_user']
            db_password = config['db_password']
            db_host = config['db_host']
            db_port = config['db_port']
            db_name = config['db_name']
            db_loc: str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            return db_loc

    except FileNotFoundError:
        print(f"Database config file not found at {config_path}.")


if __name__ == "__main__":
    db_loc = load_db_config()
    while True:
        ans = input("Do you want to update the database with new data? Answer with Yes/y or No/n: ").upper()

        if ans in ["YES", "Y", "NO", "N"]:
            break
        else:
            print("Invalid input. Answer with yes/y or no/n.")

    if ans in ["YES", "Y"]:
        # run spotify extraction
        with open("spotify_config.txt", "r") as file:
            lines = file.read().splitlines()
            c_id = lines[0]
            c_secret = lines[1]
            r_uri = lines[2]
        spotify_etl.run(db_loc=db_loc, client_id=c_id, client_secret=c_secret, redirect_uri=r_uri)

        # run Acousticbrainz extraction
        with open("musicbrainz_config.txt", "r") as file:
            lines = file.read().splitlines()
            app_name = lines[0]
            email = lines[1]
        ab_etl.run(db_loc=db_loc, app_name=app_name, email=email)

    while True:
        ans2 = input("Do you want to create excel-tables using the data available in the database? Answer with Yes/y or No/n: ").upper()

        if ans2 in ["YES", "Y", "NO", "N"]:
            break
        else:
            print("Invalid input. Answer with yes/y or no/n.")

    if ans2 in ["YES", "Y"]:
        sql_to_excel.run(db_loc=db_loc)

#    while True:
#        ans3 = input("Do you want to create some simple dashboards and visualizations of the data? Answer with Yes/y or No/n: ").upper()
#
#        if ans3 in ["YES", "Y", "NO", "N"]:
#            break
#        else:
#            print("Invalid input. Answer with yes/y or no/n.")
#
#    if ans3 in ["YES", "Y"]:
#        visualizer.run(db_loc=db_loc)
