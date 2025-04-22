import spotify_etl
import ab_etl

DATABASE_LOCATION = "sqlite:///data/my_tracks.sqlite"

if __name__ == "__main__":

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
        spotify_etl.run(db_loc=DATABASE_LOCATION, client_id=c_id, client_secret=c_secret, redirect_uri=r_uri)

        # run Acousticbrainz extraction
        with open("musicbrainz_config.txt", "r") as file:
            lines = file.read().splitlines()
            app_name = lines[0]
            email = lines[1]
        ab_etl.run(db_loc=DATABASE_LOCATION, app_name=app_name, email=email)
