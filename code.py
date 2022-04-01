import json
import base64

def parse_data(record):
    
    """
    Parse and transform record data from Kinesis Data Firehose
    """
    
    #decode record data from base64 encoding
    decode_data = base64.b64decode(record['data'])
    data_json = json.loads(decode_data)
    
    #if data meet some conditions
    if data_json['status'] == "INFO" and data_json['tag'] == "PARSETAG:":
        
        #transform
        new_data = {"date": data_json["date"], 
                    "time": data_json["time"], 
                    "client_id": data_json["client_id"], 
                    "offer_id": data_json["offer_id"]}
        new_data_json = json.dumps(new_data)
        
        #encode back to base64
        encode_data = base64.b64encode(str(new_data_json).encode())
        output = {"data": encode_data.decode(), "recordId": record["recordId"], "result": "Ok"}
    
    #otherwise send data to errors/ s3 bucket 
    else:
        
        new_data = {"message": "Invalid data"}
        new_data_json = json.dumps(new_data)
        encode_data = base64.b64encode(str(new_data_json).encode())
        output = {"data": encode_data.decode(), "recordId": record["recordId"], "result": "ProcessingFailed"}
        
    return output

def lambda_handler(event, context):
    
    #map each record with parse_data function and return to the firehose delivery stream 
    transform_data = list(map(parse_data, event['records'])) 

    return {"records": transform_data}
