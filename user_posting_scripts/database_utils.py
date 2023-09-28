import datetime
import json
import os
import random
import requests
import sqlalchemy

from dotenv import load_dotenv
from sqlalchemy import text
from time import sleep

# load environment variables for database credentials
load_dotenv()

# generate seed for reproducible 'random' results
random.seed(100)

class AWSDBConnector:
    '''This class contains methods for establishing a connection to a database 
    using SQLAlchemy and acquiring records from the connected database
    '''
    def __init__(self):
        self.HOST = os.getenv('RDSHOST')
        self.USER = os.getenv('RDSUSER')
        self.PASSWORD = os.getenv('RDSPASSWORD')
        self.DATABASE = os.getenv('RDSDATABASE')
        self.PORT = os.getenv('RDSPORT')
        self.pin_result = {}
        self.geo_result = {}
        self.user_result = {}
    
    def create_db_connector(self):
        '''Uses sqlalchemy.create_engine() method to generate connection engine
        using credentials contained in class attributes. Returns engine object.
        '''
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:"
            f"{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine
    
    def get_record_from_table(self, table: str, connection, row_number: int):
        '''
        Generates a query string from table name and row number arguments and
        executes that query string on a given database connection to obtain a
        row record from a database
        '''
        query_string = text(f"SELECT * FROM {table} LIMIT {row_number}, 1")
        selected_row = connection.execute(query_string)
        for row in selected_row:
            result = dict(row._mapping)
        return result
    
    def connect_and_get_records(self):
        '''
        Generates a random integer for selecting a random row from the database,
        creates a database connection, and then obtains said row record from each
        of the three tables in the database
        '''
        # generate a random row number between 0 and 11000
        random_row = random.randint(0, 11000)
        # create database connection
        engine = self.create_db_connector()
        with engine.connect() as connection:
            # get row record for random row from three separate tables
            self.pin_result = self.get_record_from_table("pinterest_data", connection, random_row)
            self.geo_result = self.get_record_from_table("geolocation_data", connection, random_row)
            self.user_result = self.get_record_from_table("user_data", connection, random_row)


def post_record_to_API(method: str, invoke_url: str, record_dict: dict, *args):
    '''Creates payload of correct format for posting to API, and uses
    requests library to send payload to invoke_url via PUT or POST request

    Parameters
    ----------
        method: str
            Should be 'POST' when posting to Kafka cluster, 'PUT' when posting to Kinesis stream
        invoke_url: str
            Invoke URL of API
        record_dict: dict
            Row record obtained from database
        *args:
            Should be used when posting to Kinesis stream, should be stream name string
    '''
    # iterate over record dictionary and check if any values are of type datetime
    for key, value in record_dict.items():
        # if so, convert to string
        if type(value) == datetime.datetime:
            record_dict[key] = value.strftime("%Y-%m-%d %H:%M:%S")
    # create payload from dictionary in format that can be uploaded to API
    # if optional argument is included, payload is going to Kinesis stream API
    if len(args) == 1:
        # create correct header string for request
        headers = {'Content-Type': 'application/json'}
        # create correct format of payload
        payload = json.dumps({
            "StreamName": args[0],
            "Data": record_dict,
            "PartitionKey": args[0][23:]    
        })
    # if there are no optional arguments, payload is going to Kafka batch API
    elif len(args) == 0:
        # create header string for POST request
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        payload = json.dumps({
            "records": [
                {
                    "value": record_dict
                }
            ]     
        })
    # make request to API
    response = requests.request(method, invoke_url, headers=headers, data=payload)
    print(response.status_code)


def run_infinitely(func):
    '''
    Decorator to run function infinitely at random intervals between 0 and 2
    seconds
    '''
    def inner():
        while True:
            # pause for a random length of time between 0 and 2 seconds
            sleep(random.randrange(0, 2))
            func()
    
    return inner
