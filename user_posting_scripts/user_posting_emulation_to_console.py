from database_utils import *

new_connector = AWSDBConnector()

@run_infinitely
def run_infinite_post_data_loop():
    '''
    Utilises decorator to run infinitely at random intervals, calls class method
    to get records and then prints those records to the console.
    '''
    new_connector.connect_and_get_records()
    print(new_connector.pin_result)
    print(new_connector.geo_result)
    print(new_connector.user_result)

if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()
    