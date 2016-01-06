# ----------------
# IMPORT PACKAGES
# ----------------

# Requests is a package that allows download of data from any online resource.
# The sqlite3 model is used to work with the SQLite database.
# The datetime package has datetime objects.
# The pandas package is used to fetch and store data in a DataFrame.
# The json_normalize package is to convert data into a pandas DataFrame from a JSON format.
# The collections module implements specialized container data types.
# The matplotlib package is for graphical outputs (eg. box-plot, histogram, QQ-plot).

import requests
import sqlite3 as lite
import datetime
import pandas as pd
from pandas.io.json import json_normalize
import collections
import matplotlib.pyplot as plt

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
	cur.execute("DROP TABLE IF EXISTS maxTemp")

# Create the table specifying the name of columns and their data types.
# First column wold be one month's worth of sequential dates (DATE type).
date = []
date.append(startDate)
for t in range(29):
	date.append(date[-1] + datetime.timedelta(days=1))
# Modify list into a tuple for SQL executemany() capability
dateTuple = [(t.strftime("%Y/%m/%d"),) for t in date]

# Remaining columns are the five selected cities (NUMERIC type).
# Truncate city names with spaces (ie. "Los Angeles" to "LosAngeles") for SQL convenience.
citiesTruncated = {key.replace(" ", "_"): value for key, value in cities.iteritems()}
cityId = [c + " NUMERIC" for c in citiesTruncated]

# Construct the table and add the sequential dates.
with con:
	cur.execute("CREATE TABLE maxTemp (Date INT, " + ", ".join(cityId) + ");")
	cur.executemany("INSERT INTO maxTemp (Date) VALUES (?)", dateTuple)

# Add maximum temperature data for each specified city and for each day.
# Forecast at a Given Time (https://developer.forecast.io/docs/v2#time_call)
# Format should be a string as follows: [YYYY]-[MM]-[DD]T[HH]:[MM]:[SS]
for c in citiesTruncated:
	for t in range(30):
		apiRetrieve = url + citiesTruncated[c][0] + "," + citiesTruncated[c][1] + "," + date[t].strftime("%Y-%m-%dT%H:%M:%S")
		r = requests.get(apiRetrieve)
		Tmax = float(json_normalize(r.json()["daily"]["data"])["temperatureMax"])

		# Use UPDATE query to store retrieved values
		with con:
			cur.execute("UPDATE maxtemp SET " + c + " = " + str(Tmax) + \
			" WHERE Date = " + date[t].strftime("'%Y/%m/%d'") + ";")

# ----------------
# ANALYZE DATA
# ----------------

# Analyze the average maximum temperature of each city during the specified period of time.
df = pd.read_sql_query("SELECT * FROM maxTemp ORDER BY Date", con, index_col="Date")

# ----------------
# VISUALIZE DATA
# ----------------

# Add plot, scatter plot (with fitted spline) and test auto-correlation if necessary.