# Project Summary

This project is a ETL pipeline that collects music data from various sources, including Spotify and AcousticBrainz. The pipeline is designed to extract data from these sources, transform it into a usable format, and load it into a PostgreSQL database. The project also includes tools for creating Excel tables and visualizations from the data in the database.

The pipeline is controlled by the main.py script, which prompts the user to update the database with new data and create Excel tables and visualizations. The script uses configuration files to connect to the database and authenticate with the music APIs.

The project uses several libraries, including spotipy for interacting with the Spotify API, sqlalchemy for database operations, and pandas for data manipulation. The project also includes a localserver.py script for running a local server to handle API redirects.


## Using the Project

To use this project, follow these steps:

- Install the required libraries by running pip install -r requirements.txt in your terminal.
- Create a PostgreSQL database and add the connection details to the pg_config.json file. 
- Create a Spotify App (requires an account) and add the client ID, client secret, and redirect URI to the spotify_config.txt file.
- Create an AcousticBrainz API Alias (User-Agent) and add app name and email to the musicbrainz_config.txt file.
- Beware of the formatting of the credentials - in the section below you can see how the config files should look.
- Run the main.py script to start the ETL pipeline. Follow the prompts to update the database with new data and create Excel tables and visualizations.
- Connect Power BI Desktop to the PostgreSQL database to access the data and create dashboards and visualizations.



### Configuration files

`pg_config.json`
```json
{
  "db_user": "your_database_username",
  "db_password": "your_database_password",
  "db_host": "your_database_host",
  "db_port": "your_database_port",
  "db_name": "your_database_name"
}
```

`spotify_config.txt`
```
client_id=your_spotify_client_id
client_secret=your_spotify_client_secret
redirect_uri=your_spotify_redirect_uri
```
`musicbrainz_config.txt`
```
app_name=your_musicbrainz_app_name
email=your_musicbrainz_email
```

## Project Files

### main.py

The main script that controls the ETL pipeline. Prompts the user to update the database with new data and create Excel tables and visualizations. Reads configuration files to connect to the database and authenticate with the music APIs.

### ab_etl.py

The AcousticBrainz ETL script. Extracts data from the AcousticBrainz API, transforms it into a usable format, and loads it into the database. Uses the requests library to make API calls and the sqlalchemy library to interact with the database.

### spotify_etl.py

The Spotify ETL script. Extracts data from the Spotify API, transforms it into a usable format, and loads it into the database. Uses the spotipy library to interact with the Spotify API and the sqlalchemy library to interact with the database.

### sql_to_excel.py

The script that creates Excel tables from the data in the database. Uses the pandas library to read data from the database and the openpyxl library to write data to Excel files.

### localserver.py

A simple local server script that handles API redirects. Used by the Spotify API to authenticate the user and redirect them back to the application.

## 📊 Dashboard Overview


<img src="docs/pb1.png" width="600">
<img src="docs/pb2.png" width="600">

📄 [View full PDF version](docs/summary_dashboard.pdf)