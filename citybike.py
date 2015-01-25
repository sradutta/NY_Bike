#import libraries
import requests

#get the data-set from the web
r = requests.get('http://www.citibikenyc.com/stations/json')

#explore the data-set
r.text
r.json()
r.json().keys()
r.json()['executionTime']
r.json()['stationBeanList']
len(r.json()['stationBeanList'])

#gathering all the fields together -- important for setting up the database
key_list = []
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

#getting the json data into the panda dataframe            
from pandas.io.json import json_normalize
df = json_normalize(r.json()['stationBeanList'])

#creating histogram for two of the columns
import matplotlib.pyplot as plt
import pandas as pd
df['availableBikes'].hist()
plt.show()
df['totalDocks'].hist()
plt.show()

#finding stats
df['availableBikes'].mean()  #mean is 8.1867469879518069
df['availableBikes'].median() #median is 5.5
from scipy import stats
stats.mode(df['availableBikes']) #bimodal with 1 and 40 as the modes

#finding stats for those stations that are in service
condition = (df['statusValue'] == 'In Service') #It's basically creating a column of T/F
df[condition]['availableBikes'].mean() #mean is 8.2363636363636363 and we chose only those rows that have the status-value = In service
df[df['statusValue']=='In Service']['availableBikes'].median() 
'''median is 6. Note how we didn't have to write that extra line of code condition = (df).  We could've just combined the condition and the column together as shown here'''


#creating sqlite database and the required tables
import sqlite3 as lite
con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
	cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

# prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

#this is the dynamic table whose values changes over time.
#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite since column names can't be integers
station_ids = ['_' + str(x) + ' INT' for x in station_ids] 

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

#populate the table with values
# a package with datetime objects
import time

# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 
import collections

#take the string and parse it into a Python datetime object
exec_time = parse(r.json()['executionTime'])

with con:
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))


#iterate through the stations in stationbeanlist
id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

#loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")












