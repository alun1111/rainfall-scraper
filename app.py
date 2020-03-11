import requests
import boto3
import json
from datetime import datetime
from decimal import Decimal

def getRainfall(table, stationid):
    # Get river levels csv
    r = requests.get(f'https://www2.sepa.org.uk/rainfall/api/Hourly/{stationid}')
    print('SEPA response status: ' + str(r.status_code))

    if r.status_code != 200:
        return print(f'Bad SEPA response status: {str(r.status_code)}')

    print('Successful SEPA response...')

    rainfall_data = r.json()

    with table.batch_writer(overwrite_by_pkeys=['monitoring-station-id', 'timestamp']) as batch:
        for item in rainfall_data:
            dt = datetime.strptime(item["Timestamp"],"%d/%m/%Y %H:%M:%S")
            timestamp = dt.strftime( '%Y-%m-%d %H:%M:%S')
            amount = item["Value"]

            data = json.loads(json.dumps({
                        'monitoring-station-id': stationid,
                        'timestamp': timestamp,
                        'amount': round(float(amount), 2)}), parse_float=Decimal)

            # Add to dynamodb table put batch
            batch.put_item(Item=data)

    print("Batch writing complete")

def lambda_handler(event, context):
    aws_session = boto3.Session(region_name = 'eu-west-1')
    aws_db = aws_session.resource('dynamodb')
    table = aws_db.Table('rainfall-readings')

    stationid = event.get('stationid')

    getRainfall(table, stationid) 
    
    return {
        'statusCode': 200,
        'body': json.dumps('Rainfall scraper lambda has run!')
    }
