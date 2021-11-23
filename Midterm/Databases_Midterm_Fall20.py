#!/usr/bin/env python
# coding: utf-8

# ## Databases - Fall 2020
# ### Midterm - Due Sunday, October 4 by midnight
# 
# If you would like to create views for any of these questions, please do so at the top of the section, in a cell immediately below where you connect to the database.  This will help keep the rest of your submission clean and easy to read.  Thanks!

# In[1]:


# Import any libraries you'll need here
import pandas as pd
import sqlite3


# ### Part 1) Billboard database
# 
# These questions will make use of the bb.db database which contains the Billboard song data we have seen before.
# 
# This database has two tables: tSong, and tRating.
# 
# Recall that we have code from previous exercises you can use to list out the column names for each table in the database.  You might also use the SQLite browser to help familiarize yourself with the data.

# In[2]:


# Conenct to the bb.db database
conn = sqlite3.connect('./bb.db')
curs = conn.cursor()


# In[3]:


x = pd.read_sql("""SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                AND name LIKE 't%';""",conn)
for table in x.values:
    sql = "PRAGMA table_info(" + table[0] + ");"
    print(table)
    print(pd.read_sql(sql,conn))
    print('\n')


# ___
# 1) Which songs in the database have ever made it to the top of the chart, i.e., have ever had a rating = 1?
# 
# Have your query return 3 columns: track, artist, and time.  Your results should not have any duplicate rows.

# In[4]:


pd.read_sql("""SELECT track, artist, time
                FROM tSong
                    JOIN tRating USING (id)
                WHERE rating = '1'
                GROUP BY track;""",conn)


# ___
# 2) In this database, songs are retained for 76 weeks, even if they fell off the chart and did not have a rating for all 76 consecutive weeks.
# 
# Find all artists in the database who had a song that did not last for the 76 week duration, and return a count of the number of weeks they had null ratings.
# 
# Order the results by artist name, ascending.

# In[5]:


curs.execute("DROP VIEW IF EXISTS vCountNull;")
curs.execute("""CREATE VIEW vCountNull AS 
                SELECT *
                FROM tSong
                    JOIN tRating USING (id)
                WHERE rating ISNULL
                ORDER BY artist ASC;""")


pd.read_sql("""SELECT artist, count() as WeeksNull 
                FROM vCountNull
                GROUP BY artist
                ;""", conn)


# 3) It's often good to spot check your results.  From question 2, take the first artist on the list and return:
# 
# artist, week, rating
# 
# for all entries where the rating is NULL.  The number of rows should match the number you got for this artist in question 2.

# In[6]:


pd.read_sql("""SELECT artist, week, rating
                FROM tSong
                    JOIN tRating USING (id)
                WHERE artist LIKE "2 pac"
                AND rating ISNULL

;""", conn)


# ___
# 4) What is the average rating for songs that are in week 10 of being on the Billboard chart?
# 
# _Note: Make sure that NULL ratings are not included in your average!  Do you need to add an additional condition in your query for this?_

# In[7]:


curs.execute("DROP VIEW IF EXISTS vWeek10;")
curs.execute("""CREATE VIEW vWeek10 AS 
                SELECT *
                FROM tSong
                JOIN tRating USING (id)
                WHERE rating IS NOT NULL
                AND week LIKE 'wk10'
                ;""")





pd.read_sql("""SELECT AVG(rating) FROM vWeek10;""",conn)


# ___
# 5) How many unique tracks in the database are there that are longer than 5 minutes?
# 
# Have your query return a single column with a single row: the number of songs.
# 
# _Hint: To verify your result, you might also try listing them out._

# In[8]:


curs.execute("DROP VIEW IF EXISTS vLongSongs;")
curs.execute("""CREATE VIEW vLongSongs AS 
                SELECT *
                FROM tSong
                JOIN tRating USING (id)
                WHERE time > "5:00"
                GROUP BY track;""")


pd.read_sql("""SELECT count(*) as NumSongs
                FROM vLongSongs;""",conn)


# ---
# 6) How many songs only had (non-null) ratings for a single week, and what are they?
# 
# Have your query return a list of these songs with: year, artist, track, time, date_entered, week, rating

# In[9]:


curs.execute("DROP VIEW IF EXISTS vWk2Ratings;")
curs.execute("""CREATE VIEW vWk2Ratings AS 
                SELECT *
                FROM tSong
                JOIN tRating USING (id)
                WHERE week ='wk2'
                And rating ISNULL;""")

pd.read_sql("""SELECT year, artist, track, time, date_entered, rating FROM vWk2Ratings;""",conn)
                
                


# In[10]:


# Don't forget to close your connection to the database!
conn.close()


# ### Part 2) Census database
# 
# These questions make use of the Census.db database.  This is real data, albeit a bit out of date, from the US Census Bureau regarding things such as housing, income, employment, and population broken down by county, state, and year.
# 
# This database contains 4 tables. I have listed the columns below which we will be using.  Other columns may be safely ignored.
# 
#    - **tCounty**
#        - county_id: a number which uniquely identifies each county
#        - county: the name of the county
#        - state
#        - _Note: this is the ONLY table which is guaranteed to contain ALL counties in the data._
#    - **tHousing** 
#        - county_id: same as county_id above.
#        - year
#        - units: An estimate of housing units (houses, apartments, etc. Check the census website for a more precise definition)
#    - **tEmployment**
#         - county_id: same as in the previous tables
#         - year
#         - pop: An estimate of the adult population (i.e. the available workforce)
#         - unemp_rate: The unemployment rate, expressed as a percentage, e.g. 5.0 = 5% = 0.05
#    - **tIncome**
#        - county_id: same as in the previous tables
#        - year
#        - median_inc: median income
#        - mean_inc: average (mean) income

# In[11]:


# Connect to the Census.db database
conn = sqlite3.connect('./Census.db')
curs = conn.cursor()


# In[12]:


x = pd.read_sql("""SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                AND name LIKE 't%';""",conn)
for table in x.values:
    sql = "PRAGMA table_info(" + table[0] + ");"
    print(table)
    print(pd.read_sql(sql,conn))
    print('\n')


# ---
# 7) In many places, the median income is less than the mean income, due to a relatively small number of individuals who make vastly more than the rest of the population.
# 
# Find all instances in this database where the opposite is true, that is, the median income is greater than the mean income.
# 
# Return four columns: county name, state, year, median income, mean income.

# In[13]:


pd.read_sql("""SELECT county, state, year, median_inc, mean_inc
                FROM tCounty
                    JOIN tIncome USING (county_id)
                WHERE mean_inc < median_inc;""", conn)


# ___
# 8) Assuming that population * unemployment rate = number of unemployed people, return a list of states with the highest number of unemployed people for the most recent year in the database
# 
# Have your query return five columns: state, year, population, unemployment rate, number of unemployed people.  Limit the result to the top 10, sorted in descending order.
# 
# _Note: Don't forget that the unemployment rates are expressed as percentages. A good sanity check here is that the number of unemployed people should be less than the population!_

# In[14]:


pd.read_sql("""SELECT state, year, pop, unemp_rate, 
                                0.01*pop*unemp_rate as NumUnemp
                FROM tCounty AS A
                    JOIN tEmployment AS B ON A.county_id = B.county_id                    
                WHERE year = '2017'
                GROUP BY state
                ORDER BY NumUnemp DESC
                LIMIT 10;""", conn)


# ---
# 9) Not all data exists for every county and every year in this database.  Find all counties in Virginia that are missing population data.
# 
# Have your query return two columns: state, county name

# In[15]:


pd.read_sql("""SELECT state, county 
                FROM tCounty
                    JOIN tEmployment USING (county_id)
                WHERE state LIKE 'virginia'
                AND pop = 'N';""", conn)


# ---
# 10) Find all counties where the number of housing units was less in 2017 than it was in 2015.
# 
# Have your query return 4 columns: state, county name, 2015 housing units, 2017 housing units.

# In[17]:


#PRobably need to set up two views to point to the 2015 housing units and the 2017 ones



curs.execute("DROP VIEW IF EXISTS v2017Units;")
curs.execute("""CREATE VIEW v2017Units AS
                    SELECT state, county_id, units
                    FROM tCounty
                        JOIN tHousing USING (county_id)
                    WHERE year = '2017';""")
curs.execute("DROP VIEW IF EXISTS v2015Units;")
curs.execute("""CREATE VIEW v2015Units AS
                    SELECT state, county_id, units
                    FROM tCounty
                        JOIN tHousing USING (county_id)
                    WHERE year = '2015';""")

#create table with state, conunty, 2015 units and 2017 units 
#pd.read_sql("""SELECT * FROM v2015Units;""",conn)

pd.read_sql("""SELECT tCounty.state, tCounty.county, A.units as "2015_units", B.units as "2017_units"
              FROM tCounty
              JOIN v2015Units as A USING (county_id)
              JOIN v2017Units as B USING (county_id)
              WHERE A.units > B.units
            ;""", conn)


# ---
# 11) Every town has a Main Street. There's a Miami in Florida and Ohio. There's a Roswell in New Mexico and Georgia.
# 
# Find all county names that exist in more than one state.
# 
# Have your query return two columns: county name, number of states it exists in.  Order your results with the most frequently occurring county name at the top.

# In[18]:


pd.read_sql("""SELECT county, count(*) as CountyCount
                FROM tCounty
                
                GROUP BY county
                HAVING CountyCount > 1
                ORDER BY CountyCount DESC
                ;""",conn)


# In[19]:


# Don't forget to close the connection to the database!
conn.close()


# ### Part 3) Conceptual Questions
# 
# ---
# 12) What are the rules of tidy data?

# Each variable forms a column <br>
# Each observation forms a row<br>
# Each observational unit forms a table<br>
# 
# 

# ---
# 13) What normal form does Tidy Data most closely approximate?

# Codd's Third Form

# ---
# 14) In SQLite the RIGHT JOIN operation does not exist.  Rewrite the following statement so that it would execute in SQLite: <br>
# 
# SELECT column1,column2 <br>
# FROM TableA <br>
# RIGHT JOIN TableB <br>
# ON TableB.id = TableA.id<br>
# <br>
# <br>
# 
# 
# 

# SELECT column1,column2 <br>
# FROM TableB <br>
# JOIN Table A USING (id)<br>
# 

# ---
# 15) Suppose you have the following two tables:
# <p style="text-align: center;"> TableA </p>
# 
# | x | y    |
# |---|------|
# | 1 | cat  |
# | 2 | dog  |
# | 3 | bird |
# | 4 | cow  |
# 
# <p style="text-align: center;"> TableB </p>
# 
# | x | z    |
# |---|------|
# | 2 | blue  |
# | 3 | red  |
# | 4 | brown |
# 
# and assume that we will be joining the tables on 'x'. Write a SQL statement that would produce the following output:
# 
# | x | y    | z     |
# |---|------|-------|
# | 1 | cat  | NULL  |
# | 2 | dog  | blue  |
# | 3 | bird | red   |
# | 4 | cow  | brown |

# 
# 
# pd.read_sql("""SELECT x, y, z <br>
#                 FROM TABLEA <br>
#                     JOIN TABLEB USING(x) <br>
#                     
#                 ;""",conn)
#                 

# ---
# 16) What is a Primary Key?

# A primary key is a field in a table which uniquely identifies each row/record in a database table. Primary keys must contain unique values. A primary key column cannot have NULL values.<br>
# A minimal set of columns (attributes) needed to uniquely identify and observation (row).

# ---
# 17) Database normalization and Tidy Data have several benefits, but one of the main goals is to prevent certain things from occurring.  What are those things called?

# They aim to prevent redundencies, anomalies, and inconsistencies.
# 
# 

# In[ ]:





# In[ ]:




