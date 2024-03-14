import networkx as nx
import pandas as pd 
from sqlalchemy import create_engine, text, sql
import sqlalchemy
from pandasql import sqldf
import boto3
from datetime import datetime, timedelta
from queries import generate_total_data_query


#function to generate the graph
def generate_graph(type_):
    # Making the graph
    if type_ == "orders":
        causal_graph = nx.DiGraph([
            ('wth_avail' , 'bistro_sales'), ('wth_avail' , 'frozen_sales'), ('wth_avail' , 'fnv_sales'), ('wth_avail' , 'grain_flour_sales'), ('wth_avail' , 'instant_sales') ,
            ('wth_avail' , 'personal_home_sales'), ('wth_avail' , 'spices_sales') ,
            ('bistro_sales' , 'quantity')  ,('bistro_sales' , 'orders') , ('bistro_sales' , 'gmv') ,
            ('frozen_sales' ,'quantity') ,('frozen_sales' ,'orders') ,('frozen_sales' ,'gmv') ,
            ('fnv_sales' ,'quantity') ,('fnv_sales','orders') ,('fnv_sales' ,'gmv') ,
            ('grain_flour_sales' ,'quantity') ,('grain_flour_sales' ,'orders') ,('grain_flour_sales' ,'gmv') ,
            ('instant_sales' ,'quantity') ,('instant_sales' ,'orders') ,('instant_sales' ,'gmv') ,
            ('personal_home_sales' ,'quantity') ,('personal_home_sales' ,'orders') ,('personal_home_sales' ,'gmv') ,
            ('spices_sales' ,'quantity') ,('spices_sales' ,'orders') ,('spices_sales' ,'gmv') ,
            ('quantity' ,'ipc') , ('quantity' ,'unique_skus') ,
            ('orders' ,'ipc') , ('orders' ,'unique_skus') ,
            ('orders' ,'aov'),
            ('gmv','aov')
        ])
        return causal_graph
    elif type_ == "retention":
        causal_graph = nx.DiGraph([
            ('new_users_share' , 'retention') ,('new_user_retention' ,'retention') ,
            ('existing_users_share' , 'retention') ,('existing_user_retention' ,'retention') 
        ])
        return causal_graph
    else:
        print("type of graph is not currently implemented!")
        pass



#function to generate the dataframes
def generate_data(type_ , engine ,inputs_dict):
    #Other types to be implemented : sku , monthly dynamics ,
    with engine.connect() as conn, conn.begin():
        total_data_query = generate_total_data_query(type_ = type_ , inputs_dict = inputs_dict)
        df = pd.DataFrame()
        df = pd.read_sql(total_data_query, conn)
        conn.close()
    df.fillna(0 , inplace = True)
    return df

#function to return max date from the data in the database
def get_max_date(engine):
    final_date = datetime.today().strftime('%Y-%m-%d')
    with engine.connect() as conn, conn.begin():
        df = pd.DataFrame()
        df = pd.read_sql('''select max(date) as max_date from order_item''', conn)
        conn.close()  
    final_date = pd.to_datetime(df['max_date'], format='%Y-%m-%d')
    return final_date