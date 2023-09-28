from database_utils import *

new_connector = AWSDBConnector()

@run_infinitely
def run_infinite_post_data_loop():
    '''
    Utilises decorator to run infinitely at random intervals, calls class method
    to get records and then posts the records to the Kafka cluster.
    '''
    new_connector.connect_and_get_records()
    # post result to Kafka cluster via API
    post_record_to_API("POST", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/topics/1215be80977f.pin", new_connector.pin_result)
    post_record_to_API("POST", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/topics/1215be80977f.geo", new_connector.geo_result)
    post_record_to_API("POST", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/topics/1215be80977f.user", new_connector.user_result)


if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()
    


    