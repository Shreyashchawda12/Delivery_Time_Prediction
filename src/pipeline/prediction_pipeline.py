import sys
import os
from src.exception import CustomException
from src.logger import logging
from src.utils import load_object
import pandas as pd


class PredictPipeline:
    def __init__(self):
        pass

    def predict(self,features):
        try:
            preprocessor_path=os.path.join('artifacts','preprocessor.pkl')
            model_path=os.path.join('artifacts','model.pkl')

            preprocessor=load_object(preprocessor_path)
            model=load_object(model_path)

            data_scaled=preprocessor.transform(features)

            pred=model.predict(data_scaled)
            return pred
            

        except Exception as e:
            logging.info("Exception occured in prediction")
            raise CustomException(e,sys)
        
class CustomData:
    def __init__(self,                 
                 Delivery_person_Age:int,
                 Delivery_person_Ratings:int,
                 Restaurant_latitude:float,
                 Restaurant_longitude:float,
                 Delivery_location_latitude:float,
                 Delivery_location_longitude:float,
                 Vehicle_condition:float,
                 Weather_conditions:str,
                 Road_traffic_density:str,
                 City:str):
        
        self.Delivery_person_Age=Delivery_person_Age
        self.Delivery_person_Ratings=Delivery_person_Ratings
        self.Restaurant_latitude=Restaurant_latitude
        self.Restaurant_longitude=Restaurant_longitude
        self.Delivery_location_latitude=Delivery_location_latitude
        self.Delivery_location_longitude=Delivery_location_longitude
        self.Vehicle_condition=Vehicle_condition
        self.Weather_conditions=Weather_conditions
        self.Road_traffic_density=Road_traffic_density
        self.City=City

    def get_data_as_dataframe(self):
        try:
            custom_data_input_dict = {
                'Delivery_person_Age':[self.Delivery_person_Age],
                'Delivery_person_Ratings':[self.Delivery_person_Ratings],
                'Restaurant_latitude':[self.Restaurant_latitude],
                'Restaurant_longitude':[self.Restaurant_longitude],
                'Delivery_location_latitude':[self.Delivery_location_latitude],
                'Delivery_location_longitude':[self.Delivery_location_longitude],
                'Vehicle_condition':[self.Vehicle_condition],
                'Weather_conditions':[self.Weather_conditions],
                'Road_traffic_density':[self.Road_traffic_density],
                'City':[self.City]
                }
            df = pd.DataFrame(custom_data_input_dict)
            logging.info('Dataframe Gathered')
            return df
        except Exception as e:
            logging.info('Exception Occured in prediction pipeline')
            raise CustomException(e,sys)
        