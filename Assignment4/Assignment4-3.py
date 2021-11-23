#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import pandas as pd


# In[3]:


conn = sqlite3.connect('Store.db')
curs = conn.cursor()
curs.execute("PRAGMA foreign_keys=ON;")

# Get some customer data we can use for testing
data = pd.read_csv('./data/Sales_201901.csv', dtype={'zip':str})


# In[1]:


data1 = pd.read_csv('./data/Sales_201901.csv', dtype={'zip':str})
data2 = pd.read_csv('./data/Sales_201902.csv', dtype={'zip':str})
data3 = pd.read_csv('./data/Sales_201903.csv', dtype={'zip':str})
data4 = pd.read_csv('./data/Sales_201904.csv', dtype={'zip':str})
data5 = pd.read_csv('./data/Sales_201905.csv', dtype={'zip':str})
data6 = pd.read_csv('./data/Sales_201906.csv', dtype={'zip':str})
data7 = pd.read_csv('./data/Sales_201907.csv', dtype={'zip':str})
data8 = pd.read_csv('./data/Sales_201908.csv', dtype={'zip':str})
data9 = pd.read_csv('./data/Sales_201909.csv', dtype={'zip':str})
data10 = pd.read_csv('./data/Sales_201910.csv', dtype={'zip':str})
data11 = pd.read_csv('./data/Sales_201911.csv', dtype={'zip':str})
data12 = pd.read_csv('./data/Sales_201912.csv', dtype={'zip':str})
data13 = pd.read_csv('./data/Sales_202001.csv', dtype={'zip':str})
data14 = pd.read_csv('./data/Sales_202002.csv', dtype={'zip':str})
data15 = pd.read_csv('./data/Sales_202003.csv', dtype={'zip':str})
data16 = pd.read_csv('./data/Sales_202004.csv', dtype={'zip':str})
data17 = pd.read_csv('./data/Sales_202005.csv', dtype={'zip':str})
data18 = pd.read_csv('./data/Sales_202006.csv', dtype={'zip':str})
data19 = pd.read_csv('./data/Sales_202007.csv', dtype={'zip':str})
data20 = pd.read_csv('./data/Sales_202008.csv', dtype={'zip':str})
data21 = pd.read_csv('./data/Sales_202009.csv', dtype={'zip':str})
data_list = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10, data11, data12, data13, data14, data15, data16, data17, data18, data19, data20, data21]


# In[5]:


import Store


# In[6]:



Store.Rebuild()


# In[8]:



#getcustomer id  from test loading sales data
def GetCustomerID(first_name,last_name,address,zip_code):
    '''Function will check if a record for customer exists.
        If so, return the customer id
        If multiple records are found, print a warning and return None
        If no record exists, create one and return the customer id.'''
    
    sql = """SELECT cust_id
                FROM tCust
                WHERE first_name = ?
                AND last_name = ?
                AND address = ?
                AND zip = ?;"""
    # Make sure to convert zip to string
    cust = pd.read_sql(sql, conn, params=(first_name,last_name,address,str(zip_code)))
    
    # There should only be at most, one result
    if len(cust) > 1:
        print('Found multiple customers: ' + str(len(cust)))
        return None
    
    # If the customer did not exist, then create it
    if len(cust) == 0:
        sql_insert = """INSERT INTO tCust (first_name,last_name,address,zip) VALUES (?,?,?,?);"""
        curs.execute(sql_insert, (first_name,last_name,address,str(zip_code)))
        cust = pd.read_sql(sql, conn, params=(first_name,last_name,address,str(zip_code)))
    
    return cust['cust_id'][0]


# In[9]:


##get order id from test loading sales data
def GetOrderID(cust_id, day, month, year):
    # Check to see if an order already exists for this customer/day
    sql_check_order = """SELECT order_id
                            FROM tOrder
                            WHERE cust_id = ?
                            AND day = ?
                            AND month = ?
                            AND year = ?;"""
    order_id = pd.read_sql(sql_check_order, conn, 
                           params=(cust_id, day, month, year))
    
    if len(order_id) == 0:
        # Enter the order
        sql_enter_order = """INSERT INTO tOrder (cust_id, day, month, year) 
                                VALUES (?,?,?,?);"""
        curs.execute(sql_enter_order, (cust_id, day, month, year))
        order_id = pd.read_sql(sql_check_order, conn, 
                               params =(cust_id, day, month, year))
    elif len(order_id)>1:
        # You might want to make this message a bit more informative
        print('WARNING! Multiple orders found...')
        return None
    else:
        print('Order information for customer ' + str(cust_id) + 
              ' on ' + str(day) + '/' + str(month) + '/' + str(year) 
              + ' already exists')
    
    return order_id['order_id'][0]


# In[10]:



for data in data_list:
    # Append customer ids to the DataFrame
    cust = data[['first','last','addr','city','state','zip']].drop_duplicates()
    cust.head(3)

    cust_id = []
    for row in cust.values:
        cust_id.append(GetCustomerID(row[0], row[1], row[2], row[5]))
    cust['cust_id'] = cust_id
    data_with_cust = data.merge(cust, on=['first','last','addr','zip'])
    data_with_cust
    # Get all the customer id / dates
    orders  = data_with_cust[['cust_id', 'date']].drop_duplicates()
    orders[['year','month','day']] = orders['date'].str.split('-',expand=True)
    order_id = []
    for row in orders.values:
        order_id.append(GetOrderID(row[0], row[4], row[3], row[2]))
    orders['order_id'] = order_id
    data_with_cust_order = data_with_cust.merge(orders, on=['cust_id','date'])
    data_with_cust_order
    # Fill in tOrderDetail
    COL_ORDER_ID = 17
    COL_PROD_ID = 7
    COL_QTY = 10

    sql = "INSERT INTO tOrderDetail VALUES(?,?,?)"
    for row in data_with_cust_order.values:
        curs.execute(sql, (row[COL_ORDER_ID], row[COL_PROD_ID], row[COL_QTY]))
    pd.read_sql("SELECT * FROM tOrderDetail;", conn)


# import sqlite3
# import pandas as pd## DATA 311 - Fall 2020
# ### Assignment #4 - Due Friday, October 30 by midnight
# ---
# Load all of the sales data from the sales_data.zip file provided into our Store database.
# 
# - Make sure to start with a fresh, empty copy of the database.
# - Destory the sales file we were using for testing in class - only use the new data provided
# - Make sure to load the data in chronological order, so that we will all end up with the same values for order_id and cust_id
# - The data provided is for all of 2019, and the first 9 months of 2020 (21 files total).
# - The data was generated in such a way that our total sales every month are usually, but not always, increasing.  You can use this fact as a sanity check to make sure the data was loaded correctly.
# - I will be providing new sales data eventually, so make sure the loading process is seamless and easy, and make sure to thoroughly test it.
# - When loading a file, you might want to have your code move that file into a different directory once it is succesfully loaded, so that you don't accidentally try to load it again later. Let me know if you need help with that!
# 
# 
# After doing so, answer the following questions:
# 
# ___
# 1) Generate a summary, by month and year of how our store is performing.
# 
# Have your query return the following:
#  - year
#  - month
#  - Sales: total sales for the month
#  - NumOrders: number of orders placed for the month
#  - NumCust: number of distinct customers who made a purchase (i.e. only count the customer at most once per month)
#  - OrdersPerCust: average number of orders per customer (i.e. NumOrders/NumCust)
#  - SalesPerCust: average sales per customer (i.e. Sales/NumCust)
#  - SalesPerOrder: average sales per order (i.e. Sales/NumOrders)
# 
# The results should be grouped and sorted by year and month, in ascending order.  
# 
# _Keep in mind that you have data for all 12 months of 2019, and the first 9 months of 2020, so there should be 21 rows in your results. Also, watch out for integer division!_

# In[225]:


pd.read_sql("""SELECT year, month, SUM(qty*unit_price) AS Sales, count(DISTINCT order_id) AS NumOrders, count(DISTINCT cust_id) AS NumCust, 
                (count(DISTINCT order_id)/count(DISTINCT cust_id)) AS OrdersPerCust, (SUM(qty*unit_price)/count(DISTINCT cust_id)) AS SalesPerCust, 
                (SUM(qty*unit_price)/count(DISTINCT order_id)) AS SalesPerOrder 
                FROM tOrder
                JOIN tOrderDetail USING(order_id)
                JOIN tProd USING(prod_id)
                GROUP BY  year, month
                ORDER BY year ASC;""",conn)


# ___
# 
# 2) Get our total sales for all states (50 + DC and PR, so 52 records total) for **January 2019 only**.
# 
# Have your query return:
# - st:  The state abbreviation
# - state: The name of the state
# - Sales: The total sales in that state
# 
# Order the results by the state abbreviation, in ascending order.
# 
# Make sure that all states are returned even if they had no sales.  In that case, have the query return 0 instead of NaN or Null.

# In[226]:


#Remeber to put an IFNULL STATMENT IN HERE BITch BOI

pd.read_sql("""SELECT st, state, IFNULL(Sales, 0) as Sales
                FROM tState
                LEFT JOIN (SELECT *, SUM(qty*unit_price) as Sales FROM
                    tZip
                    JOIN tCust USING(zip)
                    JOIN tOrder USING (cust_id)
                    JOIN tOrderDetail USING (order_id)
                    JOIN tProd USING (prod_id)
                    WHERE month==1 AND year==2019
                    GROUP BY st)
                    USING(st)
                GROUP BY st
                ORDER BY st;""",conn)


# ---
# 3) Going back to question 1, you may have noticed that our sales were not very good last month!
# 
# Generate a list of all customers who did not place an order last month (September, 2020)
# 
# Have your query return:
# 
# - cust_id
# - NumOrder: a count of the number of orders they placed last month (which should all be zero).

# In[227]:


curs.execute("""DROP VIEW IF EXISTS vCustSept2020;""")
curs.execute("""CREATE VIEW vCustSept2020 AS
                SELECT cust_id FROM tProd  
                JOIN tOrderDetail USING (prod_id)
                JOIN tOrder USING (order_id)
                JOIN tCust USING(cust_id)
                WHERE month like '9' and year like '2020'
                GROUP BY cust_id;""")


# In[228]:


pd.read_sql("""SELECT cust_id, 0 as NumOrder from tCust
                WHERE cust_id NOT IN vCustSept2020
                GROUP BY cust_id;""", conn)


# ___
# 
# 4) Using the list of customers from the last question, add two new columns to the result containing 1) each customer's average sales for months 1 through 8 of 2020, and 2) their sales for September of 2019.  Maybe we'll give that info to our sales team and see if we can do some marketing to those customers.

# In[15]:


curs.execute("""DROP VIEW IF EXISTS vCustSept2020;""")
curs.execute("""CREATE VIEW vCustSept2020 AS
                SELECT cust_id, NumOrders
                FROM tCust
                LEFT JOIN (SELECT *, count(DISTINCT order_id) AS NumOrders FROM tOrder
                JOIN  tOrderDetail USING (order_id)
                JOIN tProd USING(prod_id)
                WHERE month=9 and year = 2020
                GROUP BY cust_id)
                USING (cust_id)
                WHERE order_id IS NULL
                GROUP BY cust_id;""")
curs.execute("""DROP VIEW IF EXISTS vAveSales2020;""")
curs.execute("""CREATE VIEW vAveSales2020 AS
                SELECT *, Sales2020/8 as AvgSales2020
                FROM tCust
                JOIN (SELECT cust_id, sum(qty*unit_price) as Sales2020 FROM tOrder
                JOIN tOrderDetail USING (order_id)
                JOIN tProd USING (prod_id)
                WHERE cust_id IN (SELECT cust_id FROM vCustSept2020) AND year = 2020 and month IS NOT 9
                GROUP BY cust_id) USING (cust_id)
                GROUP BY cust_id;""")
curs.execute("""DROP VIEW IF EXISTS vSalesSept2019;""")
curs.execute("""CREATE VIEW vSalesSept2019 as
                SELECT *, SalesSept2019
                FROM tCust
                JOIN (SELECT cust_id, sum(qty*unit_price) as SalesSept2019 FROM tOrder
                JOIN tOrderDetail USING (order_id)
                JOIN tProd USING (prod_id)
                WHERE cust_id IN (SELECT cust_id FROM vCustSept2020) AND year = 2019 and month = 9
                GROUP BY cust_id) USING (cust_id)
                GROUP BY cust_id;""")

pd.read_sql("""SELECT cust_id, IFNULL(NumOrders, 0) as NumOrders, AvgSales2020, IFNULL(SalesSept2019, 0) AS SalesSept2019
                FROM vCustSept2020
                JOIN vAveSales2020 USING (cust_id)
                LEFT JOIN vSalesSept2019 USING (cust_id)
                GROUP BY cust_id;""", conn)


# ---
# 5) What is our top selling product (in terms of dollars) so far?
# 
# Have your query return:
# 
# - prod_id
# - prod_name
# - total quantity sold, based on all the data we have in the database
# - total sales, based on all the current data in the database

# In[14]:


pd.read_sql("""SELECT prod_id, prod_name, sum(qty) AS TotalQty, qty*unit_price as Sales 
                    FROM tProd
                    JOIN tOrderDetail USING (prod_id)
                    GROUP BY prod_id
                    ORDER BY Sales DESC
                    LIMIT 1;""", conn)

#tProd has prod_id and unit_price
#tOrderDetail has prod_id and qty


# In[ ]:


# Don't forget to close your connection when done!
conn.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




