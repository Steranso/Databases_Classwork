#!/usr/bin/env python
# coding: utf-8

# ## DATA 311 - Fall 2020
# ### Final Project - due Tuesday, Nov 24 by midnight

# In[73]:


import sqlite3
import pandas as pd


# In[74]:


conn = sqlite3.connect('Covid.db')
curs = conn.cursor()
curs.execute("PRAGMA foreign_keys=ON;")


# In[75]:


data1=pd.read_csv("./data/StayAtHome_KFF_Clean.csv") 
data2=pd.read_csv("./data/states.csv")
data3=pd.read_csv("./data/PopDensity.csv")
data4=pd.read_csv("./data/CovidDailyTracking_Filtered_Nov16.csv")
data5=pd.read_csv("./data/Ballotpedia_StateEmergency.csv")


# In[76]:


curs.execute("DROP TABLE IF EXISTS tState;")
# Create the state table
sql = """CREATE TABLE tState (
            state TEXT PRIMARY KEY,
            st TEXT NOT NULL UNIQUE);"""
curs.execute(sql)

# Load the state data
tState = pd.read_csv('./data/states.csv')
sql = "INSERT INTO tState VALUES (?,?);"
for row in tState.values:
    curs.execute(sql, (row[0], row[1]))


# In[77]:


curs.execute("DROP TABLE IF EXISTS tStayAtHome;")
#Creat the stay at home table (columns are when stay at home order was announced and when it was implemented: has 42 observations)
sql="""CREATE TABLE tStayAtHome (
            State TEXT REFERENCES tState(state),
            `Date Announced` DATE,
            `Effective Date` DATE);"""
curs.execute(sql)

#Load the stay at home data
tStayAtHome = pd.read_csv("./data/StayAtHome_KFF_Clean.csv")
sql = "INSERT INTO tStayAtHome VALUES (?,?,?);"
for row in tStayAtHome.values:
    curs.execute(sql, tuple(row))


# In[78]:


curs.execute("DROP TABLE IF EXISTS tStateInfo;")
#Create the table that has general inforamtiuon about the state like pop and land size
sql="""CREATE TABLE tStateInfo (
            State TEXT REFERENCES tState(state),
            pop2019est INTEGER,
            LandSqMi INTEGER,
            LandSqKM INTEGER,
            PopPerSqMi INTEGER,
            PopPerSqKM INTEGER);"""
curs.execute(sql)

#Load the stay at home data
tStateInfo = pd.read_csv("./data/PopDensity.csv")
sql = "INSERT INTO tStateInfo VALUES (?,?,?,?,?,?);"
for row in tStateInfo.values:
    curs.execute(sql, tuple(row))


# In[79]:


curs.execute("DROP TABLE IF EXISTS tDailyTracker;")
#Create the daily tracker table
sql="""CREATE TABLE tDailyTracker (
            st TEXT REFERENCES tState(st),
            date_clean DATE,
            positive INTEGER,
            negative INTEGER,
            death INTEGER,
            recovered INTEGER,
            positiveIncrease INTEGER,
            negativeIncrease INTEGER,
            deathIncrease INTEGER,
            totalTestResults INTEGER,
            totalTestResultsIncrease INTEGER,
            hosptitalizedCurrently INTEGER,
            hospitalizedCumulative INTEGER,
            hospitalizedIncrease INTEGER,
            inIcuCurrently INTEGER,
            inIcuCumulative INTEGER,
            onVentilatorCurrently INTEGER,
            onVentilatorCumulative INTEGER);"""
curs.execute(sql)

#Load the daily tracker data
tDailyTracker = pd.read_csv("./data/CovidDailyTracking_Filtered_Nov16.csv")
sql = "INSERT INTO tDailyTracker VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
for row in tDailyTracker.values:
    curs.execute(sql, tuple(row))


# In[80]:


curs.execute("DROP TABLE IF EXISTS tStateEmergency;")
#Creat the stay at home table (columns are when stay at home order was announced and when it was implemented: has 42 observations)
sql="""CREATE TABLE tStateEmergency (
            State TEXT REFERENCES tState(state),
            StateEmergency DATE,
            FirstSchoolClosure DATE);"""
curs.execute(sql)

#Load the stay at home data
tStateEmergency = pd.read_csv("./data/Ballotpedia_StateEmergency.csv")
sql = "INSERT INTO tStateEmergency VALUES (?,?,?);"
for row in tStateEmergency.values:
    curs.execute(sql, tuple(row))


# In[81]:


pd.read_sql("SELECT * FROM sqlite_master;", conn)


# ___
# 1) Which state(s) were the first to issue a state of emergency, and how many positive test cases had been reported in those state(s) at that time?
# 
# Return three columns:
# - State
# - Number of positive tests
# - Date of emergency declaration

# In[86]:


#Need to create a view that points to each date that a state issued a state of emergency, and then see the number of positive cases for those dates
#This view is going to need a left join because not all states issued a state of emergency
curs.execute("""DROP VIEW IF EXISTS vFirstState;""")
curs.execute("""CREATE VIEW vFirstState AS
                SELECT * FROM tStateEmergency  
                ORDER BY StateEmergency ASC
                LIMIT 1;""")


# In[87]:


#Lets Create another view where we find the info for washington on 2020-02-29 from tDailyTracker
curs.execute("""DROP VIEW IF EXISTS vWashData;""")
curs.execute("""CREATE VIEW vWashData AS
                SELECT * FROM tDailyTracker  
                WHERE st = 'WA'
                ;""")


# In[89]:


#Need to get the tDailyTracker table working so that i an get the number of positives, everything else should be correct
#Number one
pd.read_sql("""SELECT state,StateEmergency, positive
                FROM vFirstState
                JOIN tState USING (state)
                JOIN tDailyTracker USING (st)
                WHERE date_clean = '2020-02-29'
               ;""",conn)


# ___
# 
# 2) Of states which did declare a state of emergency, which were the last, how many DEATHS had been reported in those state(s) at that time, and, if they did issue a statewide stay at home order, when?
# 
# Return 4 columns:
# 
# - State
# - Number of deaths at the time state of emergency was declared
# - Date the state of emergency was declared
# - Date stay-at-home order issued (if it exists)

# In[90]:


#This will require a view where we order the tStateEmergency table from latest date to eatliest, and then pick the ones from that last date
curs.execute("""DROP VIEW IF EXISTS vLastState;""")
curs.execute("""CREATE VIEW vLastState AS
                SELECT * FROM tStateEmergency  
                ORDER BY StateEmergency DESC
                LIMIT 1;""")
#West Virginia was the last state to declare a state of emergency, one day after maine and oklahoma 


# In[91]:


#GOING TO need to create another view that points to the day that they declared the state of emergency to get the deaths number
curs.execute("""DROP VIEW IF EXISTS vDeathCount;""")
curs.execute("""CREATE VIEW vDeathCount AS
                SELECT date_clean, st, death 
                FROM tDailyTracker  
                WHERE date_clean = '2020-03-16'
                AND st = "WV";""")


# In[92]:


#Using this info want to state, deaths at time, emergency dat and stay at home date
#Number 2
pd.read_sql("""SELECT state, death, StateEmergency, `Date Announced`
                FROM vLastState
                JOIN tState USING (state)
                JOIN vDeathCount USING (st)
                JOIN tStayAtHome USING (state)
                ;""",conn)


# ___
# 3) According to the data provided, which state(s) did not issue a stay-at-home order, and how many total deaths have been reported in those state(s)?
# 
# Return two columns:
# - State
# - Number of deaths (as of Nov 15)

# In[65]:


#Need to do like a left join of the state data to the csv of csv of stay at homes and find the ones that are null, then join 
curs.execute("""DROP VIEW IF EXISTS vNoStayAtHome;""")
curs.execute("""CREATE VIEW vNoStayAtHome AS
                SELECT *
                FROM tState
                LEFT JOIN tStayAtHome USING (state)
                WHERE `Date Announced` IS NULL;""")


# In[133]:


#Number 3
pd.read_sql("""SELECT state, death
                FROM vNoStayAtHome
                JOIN tDailyTracker USING (st)
                WHERE date_clean = '2020-11-15'
                
                
                ;""",conn)


# ___
# 4) Repeat the previous question, but this time look at states that did issue a stay-at-home order
# 
# Return three columns:
# - State
# - Number of deaths (as of Nov 15)
# - Date stay-at-home order announced

# In[218]:


curs.execute("""DROP VIEW IF EXISTS vYesStayAtHome;""")
curs.execute("""CREATE VIEW vYesStayAtHome AS
                SELECT *
                FROM tState
                LEFT JOIN tStayAtHome USING (state)
                WHERE `Date Announced` IS NOT NULL;""")


# In[221]:


#Number 4

pd.read_sql("""SELECT state, death, `Date Announced`
                FROM tStayAtHome
                JOIN tState USING (state)
                JOIN tDailyTracker USING (st)
                WHERE date_clean = 2020-11-15'              
                
                
                ;""",conn)


# ___
# 5) Return the following statistics for Virginia:
# - Total number of postive cases reported
# - Total number of deaths
# - Total number of deaths per capita
# - Mortality rate, estimated by: Number of deaths / number of positive cases
# 
# _Hint: Beware of data types, integer conversion etc. The answers are probably not zero._

# In[263]:


#Just have to use the last set of data from the covid tracker, so the 15th and then where st = "VA"
#I think this can work using just the tDailyTrakcer and tStateInfo (for population data)
#LEts create a view that just points to virginia on the 15th of november, the last day with inforation
curs.execute("""DROP VIEW IF EXISTS vVADeaths;""")
curs.execute("""CREATE VIEW vVADeaths AS
                SELECT *
                FROM tDailyTracker
                WHERE st="VA"
                AND date_clean="2020-11-15";""")


# In[265]:


#NExt We are going to create a view that points at the virginia data in tStateInfo, so that we can get the returns tat require caluclations
curs.execute("""DROP VIEW IF EXISTS vVAInfo;""")
curs.execute("""CREATE VIEW vVAInfo AS SELECT * FROM tStateInfo WHERE state="Virginia";""")


# In[268]:


pd.read_sql("""SELECT positive, death, (1.0*death/pop2019est) AS DeathPerCapita, (1.0*death/positive) AS MortalityRate
                FROM vVADeaths
                JOIN tState USING (st)
                JOIN vVAInfo USING (State)
                ;""",conn)


# ___
# 6) Which state has had the most deaths per capita as of Nov 15?
# 
# Return: 
# 
# - State
# - Number of deaths
# - Population
# - Population per square mile
# - Number of deaths per capita
# - Mortality rate, estimated as 
# 
# _Hint: I made a view first, which shortened up the SQL here quite a bit_

# In[24]:


#Create a view , this info can all come from tStateInfo and tDailyTracker

curs.execute("""DROP VIEW IF EXISTS vMostDeaths;""")
curs.execute("""CREATE VIEW vMostDeaths AS
                SELECT *
                FROM tStateInfo
                JOIN tState USING (state)
                JOIN tDailyTracker USING (st)
                WHERE date_clean = "2020-11-15"
                ;""")


# In[25]:


#death per capita, mortality rate as deaths/positive cases
pd.read_sql("""SELECT State, death, pop2019est, PopPerSqMI, (1.0*death/pop2019est) AS DeathsPerCapita, (1.0*death/positive) AS MortalityRate
               FROM vMostDeaths
               ORDER BY DeathsPerCapita DESC
                LIMIT 1
                
                
                ;""",conn)


# ___
# 7) Repeat the previous question, but this time for the state with the fewest deaths per capita as of Nov 15

# In[27]:


pd.read_sql("""SELECT State, death, pop2019est, PopPerSqMI, (1.0*death/pop2019est) AS DeathsPerCapita, (1.0*death/positive) AS MortalityRate
               FROM vMostDeaths
               ORDER BY DeathsPerCapita ASC
                LIMIT 1;""",conn)


# ___
# 8) For the entire US (i.e. the sum of all 50 states + Washington DC):
# 
# Get the daily number (not the running total) of positive cases, deaths, and tests reported
# 
# Return: 
# 
# - Date
# - The number of new positive tests reported per day
# - The number of new deaths reported per day
# - The number of new tests performed per day
# 
# Order the results by date, ascending

# In[82]:


pd.read_sql("""SELECT date_clean, SUM(positiveIncrease), SUM(deathIncrease), SUM(totalTestResultsIncrease)
                FROM tDailyTracker
                GROUP BY date_clean;""",conn)


#May need to group by date clean


# ___
# BONUS: +2 points.  The previous results aren't readily intepretable as a long list of numbers.
# 
# Make a plot with date on the x-axis, and daily # of deaths on the y-axis.
# 

# In[83]:


bonus_df = pd.read_sql("""SELECT date_clean, SUM(positiveIncrease), SUM(deathIncrease), SUM(totalTestResultsIncrease)
                FROM tDailyTracker
                GROUP BY date_clean;""",conn)


# In[85]:


bonus_df.plot(x ='date_clean', y='SUM(deathIncrease)', kind = 'line')


# In[1]:


# Don't forget to close the database!
conn.close()

