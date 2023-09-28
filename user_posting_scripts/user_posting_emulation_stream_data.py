from database_utils import *

new_connector = AWSDBConnector()

@run_infinitely
def run_infinite_post_data_loop():
    '''
    Utilises decorator to run infinitely at random intervals, calls class method
    to get records and then posts the records to the Kinesis streams.
    '''
    new_connector.connect_and_get_records()
    # post result to Kinesis stream via API
    post_record_to_API("PUT", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-pin/record", new_connector.pin_result, "streaming-1215be80977f-pin")
    post_record_to_API("PUT", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-geo/record", new_connector.geo_result, "streaming-1215be80977f-geo")
    post_record_to_API("PUT", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-user/record", new_connector.user_result, "streaming-1215be80977f-user")    
            

if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()
    
