import json
import base64

def parse_and_transform(record):
    
    """
    Parse and transform record data from Kinesis Data Firehose stream.
    These example assume that input record comes in json format with keys: "date", "time", "client_id", "offer_id", etc.
    For the simplicity of the example, we will select some keys from the record and return them back to the stream.
    """
    
    #decode record data from base64 encoding
    decode_data = base64.b64decode(record['data'])
    data_json = json.loads(decode_data)
    
    #if data meet some conditions
    if data_json['status'] == "INFO" and data_json['tag'] == "PARSETAG:":
        
        new_data = {"date": data_json["date"], 
                    "time": data_json["time"], 
                    "client_id": data_json["client_id"], 
                    "offer_id": data_json["offer_id"]}
        result = "Ok"
    
    #otherwise put data to errors/ prefix inside s3 bucket
    else:
        
        new_data = {"message": "Invalid data"}
        result = "ProcessingFailed"
    
    #encode back to base64
    new_data_json = json.dumps(new_data)
    encode_data = base64.b64encode(str(new_data_json).encode())
    output = {"data": encode_data.decode(), "recordId": record["recordId"], "result": result}
    
    return output

def lambda_handler(event, context):
    
    #map each record with parse_data function and return to the firehose delivery stream 
    transform_data = list(map(parse_and_transform, event['records'])) 

    return {"records": transform_data}
