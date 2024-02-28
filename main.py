##################################################################     imports    #########################################################################################

import pandas as pd
import numpy as np

import seaborn as sns
import gc
import pickle
import time
import warnings
import datetime
from sqlalchemy import create_engine
from utility_func import generate_data 
from proactive_model import * 
import psycopg2
import pickle
import datetime
from datetime import timedelta
warnings.filterwarnings('ignore')

################################################################### establishing connection to postgres database #############################################################

password_postgres = 'f15fd380627faa60a45fcc286d45f6e9610e'
username_postgres = 'adjay'
connection_string = '''postgresql://'''+f'''{username_postgres}'''+''':'''+f'''{password_postgres}'''+'''@dookan-dev.claxyccbejgz.eu-west-1.rds.amazonaws.com:5432/dookan'''
engine = create_engine(connection_string)

#######################################################   generating the datasets for total_data , normal_data , outlier_data    #############################################

store = ['EURO_STORE' ,'CZECH_STORE' , 'ALL']
source_name = ['pos' , 'online' , 'ALL']


for store in store:
    for source in source_name:
        total_data_dict = {
        'initial_date' : '2023-01-01',
        'final_date' : '2024-01-31',
        'store' : store ,
        'source_name' : source}

        total_data = generate_data(type_ = "orders", engine = engine , inputs_dict = total_data_dict)
        
        if total_data.shape[0] > 1:
            print("*************************************************************************************************************************************************")
            t = datetime.date(2024 , 1 ,31)
            t_1 = t - timedelta(days=1)
            t_7 = t - timedelta(days=7)
            t_30 = t - timedelta(days=30)

            #slicing total_data to generate the normal_data dict 
            normal_data_week = total_data[(total_data['date'] >= t_7)  & (total_data['date'] <= t_1)]
            normal_data_month = total_data[(total_data['date'] >= t_30)  & (total_data['date'] <= t_1)]
            #slicing outlier_data to generate the outlier_data dict
            outlier_data = total_data[total_data['date'] == t]


            total_data = total_data.drop(['date' , 'source_name' ,'store' ,'customers'] , axis = 1)
            normal_data_week = normal_data_week.drop(['date' , 'source_name' ,'store' ,'customers'] , axis = 1)
            normal_data_month = normal_data_month.drop(['date' , 'source_name' ,'store' ,'customers'] , axis = 1)
            outlier_data = outlier_data.drop(['date' , 'source_name' ,'store' ,'customers'] , axis = 1)

            ########################################################    running the model        ##################################################################################

            my_model = Proactive_model(type_ = "orders" , total_data = total_data)
            #my_model.view_graph()
            print(f"Store = {store} and Source_name = {source}")
            print("From this week:")
            my_model.generate_attributes(target_node = "orders" , normal_data = normal_data_week , outlier_data = outlier_data)
            print("--------------------------------------------------------------------------------")
            print(f"Store = {store} and Source_name = {source}")
            print("From this month:")
            my_model.generate_attributes(target_node = "orders" , normal_data = normal_data_month , outlier_data = outlier_data)
        
        
        else:
            pass








