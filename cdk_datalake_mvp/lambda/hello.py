import base64
import json
import boto3
import os
from os import environ
from botocore.config import Config
from datetime import datetime

print('Loading function')



def isBase64(s):
    try:
        return base64.b64encode(base64.b64decode(s)) == s
    except Exception:
        return False

def split_func(a):
    
    string = a
    words = string.split(',')
    
    return words

def lambda_handler(event, context):
    
    def ret_locDetail(ln,lt):
    
        PLACE_INDEX_NAME = environ.get("PLACE_INDEX_NAME", os.environ['index_name'])
        location = boto3.client("location", config=Config(user_agent="Amazon Aurora PostgreSQL"))
    
        loc_input_payload={
            "Position": [ln,lt],
            "MaxResults": 1,
            "Language": "en"
            
        }
        
        kwargs = {}
        kwargs['MaxResults'] = loc_input_payload['MaxResults']
        
        locjson = location.search_place_index_for_position(IndexName=PLACE_INDEX_NAME,Position=loc_input_payload['Position'],**kwargs)['Results'][0]
    
        return locjson
        
    output = []
    client = boto3.client('sns')
    s3client = boto3.client('s3')
    bucketn = os.environ['dLake_name']

    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H:%M:%S")
    file_path='/tmp/temp-raw'+date_time+'.json'
    s3_file='swarm-raw-'+date_time+'.json'
    soh_record=0
    sensor_record=0
    unknown_record=0
    
    bucket=bucketn
    
    f = open(file_path, "a")
    
    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data']).decode('utf-8')
        payload = json.loads(payload)
        payload_b64c = base64.b64decode(payload['data'])
        if isBase64(payload_b64c):
            
            payload['data'] = base64.b64decode(payload_b64c).decode('utf-8')
            payload_s1 = json.dumps(payload, indent=0)
            if '$PIMD8' in payload_s1.strip('\n') or '$PIMD9' in payload_s1.strip('\n'):
                
                sensor_record=1
                file_path_sensor='/tmp/temp-sensor'+date_time+'.json'
                f1 = open(file_path_sensor, "a")
                
                if '$PIMD8' in payload_s1.strip('\n'):
                    
                    words=split_func(payload['data'])
                    lat=words[5]
                    lng=words[7]
                    
                    if words[6][0]=='S':
                        lat='-'+ lat
                        
                    elif words[8][0]=='W':
                        lng='-'+ lng
                    
                    print(lat)
                    print(words[6])
                    print(lng)
                    print(words[8][0])
                    
                    locjson1=ret_locDetail(float(lng),float(lat))
                    
                    alert_payload=payload
                    
                    alert_payload['AddressNumber']=locjson1['Place']['AddressNumber']
                    alert_payload['Street']=locjson1['Place']['Street']
                    alert_payload['Municipality']=locjson1['Place']['Municipality']
                    alert_payload['Region']=locjson1['Place']['Region']
                    alert_payload['SubRegion']=locjson1['Place']['SubRegion']
                    alert_payload['PostalCode']=locjson1['Place']['PostalCode']
                    alert_payload['Country']=locjson1['Place']['Country']
                    alert_payload['TimeZone_Name']=locjson1['Place']['TimeZone']['Name']
                    alert_payload['TimeZone_Offset']=locjson1['Place']['TimeZone']['Offset']
                    
                    alert_payload = json.dumps(alert_payload, indent=0)
                    
                    #response = client.publish(
                        #TopicArn='',
                        #Message=alert_payload,
                        #Subject='Type 8 Sensor Message Alert'
                    #)
               
                print('sensor message')
                f1.write(payload_s1)
               
            else:
                print ('unknown message')
                
                unknown_record=1
                file_path_sensor_unknown='/tmp/temp-unknown'+date_time+'.json'
                f2 = open(file_path_sensor_unknown, "a")
                f2.write(payload_s1)
        else:
            print('not encoded')
            
            soh_record=1
            
            payload['data'] = json.loads(base64.b64decode(payload['data']).decode('utf-8'))
        
            raw_payload=json.dumps(payload, indent=0)
            f.write(raw_payload)
            
            locjson=ret_locDetail(payload['data']['ln'],payload['data']['lt'])
        
            payload['packetid']=payload['packetId']
            payload['devicetype']=payload['deviceType']
            payload['deviceid']=payload['deviceId']
            payload['userapplicationid']=payload['userApplicationId']
            payload['organizationid']=payload['organizationId']
            payload['hiverxtime']=payload['hiveRxTime']
            payload['longitude']=payload['data']['ln']
            payload['latitude']=payload['data']['lt']
            payload['addressnumber']=locjson['Place']['AddressNumber']
            payload['street']=locjson['Place']['Street']
            payload['municipality']=locjson['Place']['Municipality']
            payload['region']=locjson['Place']['Region']
            payload['subregion']=locjson['Place']['SubRegion']
            payload['postalcode']=locjson['Place']['PostalCode']
            payload['country']=locjson['Place']['Country']
            payload['timezone_name']=locjson['Place']['TimeZone']['Name']
            payload['timezone_offset']=locjson['Place']['TimeZone']['Offset']
            payload['solar_panel_current']=payload['data']['si']
            payload['battery_current']=payload['data']['bi']
            payload['solar_panel_voltage']=payload['data']['sv']
            payload['battery_voltage']=payload['data']['bv']
            payload['date_time']=payload['data']['d']
            payload['no_messages_sent_since_last_power_cycle']=payload['data']['n']
            payload['altitude']=payload['data']['a']
            payload['speed']=payload['data']['s']
            payload['course']=payload['data']['c']
            payload['last_rssi_value']=payload['data']['r']
            payload['modem_current']=payload['data']['ti']
        
            payload.pop('data', None)
            payload.pop('packetId', None)
            payload.pop('deviceType', None)
            payload.pop('deviceId', None)
            payload.pop('userApplicationId', None)
            payload.pop('organizationId', None)
            payload.pop('hiveRxTime', None)


            # Do custom processing on the payload here
            payload_s = json.dumps(payload, indent=0)
        
            #if payload['solar_panel_voltage']<12:
                #response = client.publish(
                #TopicArn='',
                #Message=payload_s,
                #Subject='Low Solar Voltage Alert'
                #)
            #if payload['battery_voltage']<4:
                #response = client.publish(
                #TopicArn='',
                #Message=payload_s,
                #Subject='Low Battery Alert'
                #)
        
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(payload_s.encode('utf-8')).decode('utf-8')
            }
            output.append(output_record)

    f.close()
    
    if sensor_record==1:
        
        f1.close()
        s3_sensor_file='raw-sensor-'+date_time+'.json'
        keypath='raw/sensor/' + now.strftime("%Y") + '/' + now.strftime("%m") + '/' + now.strftime("%d") +  '/' + s3_sensor_file
        s3client.upload_file(file_path_sensor, bucket, keypath)
        
        if os.path.exists(file_path_sensor):
            os.remove(file_path_sensor)
        
    
    if unknown_record==1:
        
        f2.close()
        s3_sensor_unknown_file='raw-unknown-'+date_time+'.json'
        keypath='raw/unknown/' + now.strftime("%Y") + '/' + now.strftime("%m") + '/' + now.strftime("%d") +  '/' + s3_sensor_unknown_file
        s3client.upload_file(file_path_sensor_unknown, bucket, keypath)
        
        if os.path.exists(file_path_sensor_unknown):
            os.remove(file_path_sensor_unknown)
        
    
    #print('Successfully processed {} records.'.format(len(event['records'])))
    
    if soh_record==1:
    
        keypath='raw/soh/' + now.strftime("%Y") + '/' + now.strftime("%m") + '/' + now.strftime("%d") +  '/' + s3_file
    
        s3client.upload_file(file_path, bucket, keypath)
    
        if os.path.exists(file_path):
            os.remove(file_path)

    return {'records': output}