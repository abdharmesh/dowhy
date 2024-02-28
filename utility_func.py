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
    else:
        print("type of graph is not currently implemented!")
        pass



#function to generate the dataframes 
def generate_data(type_ , engine ,inputs_dict):
    #Other types to be implemented : sku , monthly dynamics , 
    cond_str_list = []
    cond_str = ""
    for key , value in inputs_dict.items():
        if key == 'initial_date':
            cond_str_list.append(f"a.date >= '{value}'")
        
        if key == 'final_date':
            cond_str_list.append(f"a.date <= '{value}'")
        
        if key == 'store':
            cond_str_list.append(f"a.{key} = '{value}'")
        
        if key == 'source_name':
            cond_str_list.append(f"a.{key} = '{value}'")
    
        
    for i in range(len(cond_str_list)):
        cond_str += cond_str_list[i]
        if i != len(cond_str_list)-1 :
            cond_str += " and "


    with engine.connect() as conn, conn.begin():
        total_data_query = generate_total_data_query(cond_str=cond_str)
        df = pd.DataFrame()
        df = pd.read_sql(total_data_query, conn)
        conn.close()    
    df.fillna(0 , inplace = True)
    return df
    
