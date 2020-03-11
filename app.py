import requests
import boto3
import json
from datetime import datetime
from decimal import Decimal

def getRainfall(table, stationid):
    # Get river levels csv
    # r = requests.get(f'http://apps.sepa.org.uk/database/riverlevels/{riverid}.csv')
    r = requests.get(f'https://www2.sepa.org.uk/waterlevels/CSVs/{riverid}.csv')
    print('SEPA response status: ' + str(r.status_code))

    if r.status_code != 200:
        return print(f'Bad SEPA response status: {str(r.status_code)}')

    print('Successful SEPA response...')


    # dt = datetime.strptime(row[0],"%d/%m/%Y %H:%M:%S")
    # timestamp = dt.strftime( '%Y-%m-%d %H:%M:%S')
    # data = json.loads(json.dumps({
    #             'monitoring-station-id': riverid,
    #             'timestamp': timestamp,
    #             'depth': round(float(row[1]), 2)}), parse_float=Decimal)

    # # Add to dynamodb table put batch
    # batch.put_item(Item=data)

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
