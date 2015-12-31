# ----------------
# IMPORT PACKAGES
# ----------------

# Requests is a package that allows download of data from any online resource.
# The sqlite3 model is used to work with the SQLite database.
# The datetime package has datetime objects.
# The pandas package is used to fetch and store data in a DataFrame.
# The json_normalize package is to convert data into a pandas DataFrame from a JSON format.

import requests
import sqlite3 as lite
import datetime
import pandas as pd
from pandas.io.json import json_normalize

# ----------------
# OBTAIN DATA
# ----------------

# The Dark Sky Forecast API
# URL needs to be in "https://api.forecast.io/forecast/[API Key]/[Latitude],[Longitude]" format
apiKey = "0ffdb15cde8b38ac285a428e8764d3e1"
url = "https://api.forecast.io/forecast/" + apiKey + "/"

# Latitude and longitude coordinates of (five) selected cities
cities = {
	"Grand Rapids": ["42.963360", "-85.668086"],
	"Jakarta": ["-6.173260", "106.822815"],
	"Los Angeles": ["34.019394", "-118.410825"],
	"Salt Lake City": ["40.778996", "-111.932630"],
	"Seattle": ["47.620499", "-122.350876"]
}

# Duration of data pull from today to number of days prior
startDate = datetime.datetime.now()
# Change format from datetime.datetime(YYYY, MM, DD, HH, MM, SS, TTTTT) to datetime.datetime(YYYY, MM, DD, 0, 0)
startDate = datetime.datetime.combine(startDate.date(), datetime.time(0)) 
startDate = startDate - datetime.timedelta(days=30)

# ----------------
# STORE DATA
# ----------------

# Connect to the database. The "connect()" method returns a connection object.
con = lite.connect("weather.db")
cur = con.cursor()

# Drop currently existing tables.
with con:
	cur.execute("DROP TABLE IF EXISTS daily_temp")