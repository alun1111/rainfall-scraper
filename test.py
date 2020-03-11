from app import lambda_handler

print("Starting function...")

event = { 'stationid': '14881'}
lambda_handler(event, "")