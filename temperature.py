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

# Load collected data into a DataFrame
df = pd.read_sql_query("SELECT * FROM maxTemp ORDER BY Date", con, index_col="Date")

# Determine the city with the highest amount of temperature shift from a day-to-day basis.
tempShift = collections.defaultdict(int)
# Loop over each city column
for col in df.columns:
	# List the values of every temperature data collected for each city column.
	tempList = df[col].tolist()
	# Starting point of zero change and create loop to compare each day's temperature.
	tempShift[col] = 0
	for t in range(len(tempList) - 1):
		tempChange = abs(tempList[t] - tempList[t + 1])
		# If the absolute value of temperature shift is greater than current value, replace the current value.
		if tempChange > tempShift[col]:
			tempShift[col] = tempChange
# To determine the city key with the highest temperature shift.
tempShiftMax = max(tempShift, key=tempShift.get)
print("The city with the largest day-to-day maximum temperature shift is " 
	+ tempShiftMax.replace("_", " ") + " with a change of " + str(tempShift[tempShiftMax])
	+ " degrees."
)

# ----------------
# VISUALIZE DATA
# ----------------

# Add plot, scatter plot (with fitted spline) and test auto-correlation if necessary.