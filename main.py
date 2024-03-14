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
from utility_func import generate_data , get_max_date
from proactive_model import * 
import psycopg2
import pickle
import datetime
from datetime import timedelta ,datetime
warnings.filterwarnings('ignore')

################################################################### establishing connection to postgres database #############################################################

password_postgres = 'f15fd380627faa60a45fcc286d45f6e9610e'
username_postgres = 'adjay'
connection_string = '''postgresql://'''+f'''{username_postgres}'''+''':'''+f'''{password_postgres}'''+'''@dookan-dev.claxyccbejgz.eu-west-1.rds.amazonaws.com:5432/dookan'''
engine = create_engine(connection_string)

#######################################################   generating the datasets for total_data , normal_data , outlier_data    #############################################

store = ['EURO_STORE' ,'CZECH_STORE' , 'ALL']  # to be selected by user
source_name = ['pos' , 'online' , 'ALL'] # to be selected by user
type_ = "retention" # can be either orders or retention

#getting final date as max date from the database
final_date = get_max_date(engine = engine)
initial_date = final_date - timedelta(days=365)

for store in store:
    for source in source_name:

        total_data_dict = {
        'initial_date' : initial_date[0].strftime("%Y-%m-%d"),
        'final_date' : final_date[0].strftime("%Y-%m-%d"),
        'store' : store ,
        'source_name' : source}


        if type_ == "orders":
            total_data = generate_data(type_ = "retention", engine = engine , inputs_dict = total_data_dict)

            if total_data.shape[0] > 1:
                print("*************************************************************************************************************************************************")
                t = final_date
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
        
        elif type_ == "retention":
            total_data = generate_data(type_ = "retention", engine = engine , inputs_dict = total_data_dict)
            total_data['ret_month'] = pd.to_datetime(total_data['ret_month'])

            if total_data.shape[0] > 1:
                print("*************************************************************************************************************************************************")
                t = total_data.ret_month.nlargest(1).iloc[-1]
                t_3 = total_data.ret_month.nlargest(4).iloc[-1]
                t_12 = total_data.ret_month.nsmallest(1).iloc[-1]
                

                #slicing total_data to generate the normal_data dict
                normal_data_3_months = total_data[(total_data['ret_month'] >= t_3)  & (total_data['ret_month'] < t)]
                normal_data_year = total_data[(total_data['ret_month'] < t)]
                #slicing outlier_data to generate the outlier_data dict
                outlier_data = total_data[total_data['ret_month'] == t]


                total_data = total_data.drop(['ret_month' , 'source' ,'store'] , axis = 1)
                normal_data_3_months = normal_data_3_months.drop(['ret_month' , 'source' ,'store'] , axis = 1)
                normal_data_year = normal_data_year.drop(['ret_month' , 'source' ,'store'] , axis = 1)
                outlier_data = outlier_data.drop(['ret_month' , 'source' ,'store'] , axis = 1)

                ########################################################    running the model        ##################################################################################

                my_model = Proactive_model(type_ = "retention" , total_data = total_data)
                #my_model.view_graph()
                print(f"Store = {store} and Source_name = {source}")
                print("From last 3 months:")
                my_model.generate_attributes(target_node = "retention" , normal_data = normal_data_3_months , outlier_data = outlier_data)
                print("--------------------------------------------------------------------------------")
                print(f"Store = {store} and Source_name = {source}")
                print("From last 1 year:")
                my_model.generate_attributes(target_node = "retention" , normal_data = normal_data_year , outlier_data = outlier_data)




        else:
            pass







