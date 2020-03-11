from app import lambda_handler

print("Starting function...")

event = { 'stationid': '14867-SG'}
lambda_handler(event, "")