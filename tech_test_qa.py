
"""
You are requested to write a python script to identify the quality control issues we have
intentionally introduced into this data. Show the code you have used to run the tests you
have performed. Share any conclusions you have arrived at as comments.
"""
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

tech_test = create_engine("postgresql://candidate:NW337AkNQH76veGc@technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com:5432/technical_test")
trades = pd.read_sql("""select * from trades""",tech_test)
print(trades.head())
users = pd.read_sql("""select * from users""",tech_test)
print(users.head())

type_col = trades.dtypes
################ QA for trades table
for i in list(trades.columns):
    str_col = ['login_hash','ticket_hash','server_hash', 'symbol']
    int_col = ['digits', 'cmd','volume']
    float_col = ['open_price', 'contractsize']
    date_col = ['open_time', 'close_time']
    #Checking type of columns
    if i in str_col and trades[i].dtype.name != 'object' :
        print(i, ' String columns are not in correct format')
    
    if i in int_col and trades[i].dtype.name != 'int64' :
        print(i, 'Integer columns are not in correct format')
        if i =='cmd':
            # check cmd values lie in 0,1
            cmd_check = trades[(trades['cmd']!=0) & (trades['cmd']!=1)]
            print(cmd_check)
            if (len(cmd_check)>0):
                print('cmd out of range')
    
        
    if i in float_col and trades[i].dtype.name != 'float64' :
        print(i, 'float64 columns are not in correct format')
        
            
        
    if i in date_col:
        if trades[i].dtype.name != 'datetime64[ns]' :
            print(i, 'datetime64[ns] columns are not in correct format')
        else:
            #Check if dates are weekdays or weekend as weekend trading is closed       
            trades[i+'_weekday'] = pd.to_datetime(trades[i]).dt.day_of_week
            weekend = trades[trades[i+'_weekday']>5]
            if len(weekend)>0:
                print(i, 'has weekend')
    

#Checking if open_time is greater than close_time
trades['flag'] = np.where((trades['open_time']> trades['close_time']), 1, 0)

open_close = trades[trades['flag'] ==1]
if (len(open_close)>0):
    print('Open time > close time')

############## QA for users table 
type_col_u = users.dtypes
for i in list(users.columns):
    str_col = ['login_hash', 'server_hash', 'country_hash', 'currency']
    int_col = ['enable']
    #Checking type of columns
    if i in str_col and users[i].dtype.name != 'object' :
        print(i, ' String columns are not in correct format')
    
    if i in int_col and users[i].dtype.name != 'int64' :
        print(i, 'Integer columns are not in correct format')


enable_check = users[(users['enable']!=0) & (users['enable']!=1)]
print(enable_check)
if (len(enable_check)>0):
    print('enable out of range')
   