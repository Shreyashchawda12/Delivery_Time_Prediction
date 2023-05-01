import pandas as pd
import numpy as np
import os
import sys
from src.logger import logging
from src.exception import CustomException

def clean_csv():
    logging.info("cleaning dataset")
    try:
        df=pd.read_csv(os.path.join('Notebook/raw_data.csv'))
        df = df.drop(labels=["ID","Delivery_person_ID","Order_Date","Type_of_order"],axis=1)
        df["Time_order_hour"] = df["Time_Orderd"].str.split(":").str[0]
        df["Time_order_min"] = df["Time_Orderd"].str.split(":").str[1]
        df = df.drop(labels=["Time_Orderd"],axis=1)
        df["Time_order_picked_hour"] = df["Time_Order_picked"].str.split(":").str[0]
        df["Time_order_picked_min"] = df["Time_Order_picked"].str.split(":").str[1]
        df = df.drop(labels=["Time_Order_picked"],axis=1)
        df["Time_order_hour"] = df["Time_order_hour"].astype(float)
        df["Time_order_min"] = df["Time_order_min"].astype(float)
        df["Time_order_picked_hour"] = df["Time_order_picked_hour"].astype(float)
        df["Time_order_picked_min"] = df["Time_order_picked_min"].astype(float)
        df1 = df.copy()
        numerical_columns=df.columns[df.dtypes!='object']
        categorical_columns=df.columns[df.dtypes=='object']
        df1 = df1[numerical_columns]
        df2 = df1.fillna(value = df1.median())
        df1 = df[categorical_columns]
        df2["Weather_conditions"] = df1["Weather_conditions"].fillna(method = 'ffill')
        df2["Road_traffic_density"] = df1["Road_traffic_density"].fillna(value = 'Medium')
        df2["Festival"] = df1["Festival"].fillna(value = 'No')
        df2["City"] = df1["City"].fillna(value = 'Semi-Urban')
        x = df2.drop(labels=["Time_taken (min)"],axis=1)
        y = df2[["Time_taken (min)"]]
        df2 = x
        df2['Time_taken (min)'] = y
        df = pd.DataFrame(df2)
        df.to_csv("artifact/clean.csv",index=False,header=True)
        
        logging.info('Data cleaning completed')
        
        return df

    
    
    except Exception as e:
            logging.info('Exception at Data cleaning stage')
            raise CustomException(e,sys)
        
    
clean_csv = clean_csv()
clean_csv

        
    