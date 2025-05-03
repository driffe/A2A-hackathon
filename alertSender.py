import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from twilio.rest import Client

# Load environment variables
load_dotenv()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def send_alert(alert_data: dict) -> dict:
    """
    Send SMS alert
    
    Args:
        alert_data: Alert data
            - user_id: User ID
            - alert_type: Alert type (no_response, medication_not_taken)
            - message: Alert message
            - recipients: List of recipient phone numbers
            
    Returns:
        dict: Alert sending result
    """
    try:
        logger.info(f"Starting alert sending: {alert_data.get('alert_type')}")
        logger.info(f"Environment variables - SID: {bool(TWILIO_ACCOUNT_SID)}, Token: {bool(TWILIO_AUTH_TOKEN)}, Phone: {TWILIO_PHONE_NUMBER}")
        
        message = alert_data.get("message")
        recipients = alert_data.get("recipients", [])
        
        logger.info(f"Message to send: {message}")
        logger.info(f"Recipients: {recipients}")
        
        # Initialize Twilio client
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        
        # Store results
        results = {}
        
        # Send SMS
        for recipient in recipients:
            try:
                logger.info(f"Attempting to send SMS to: {recipient}")
                
                # Send SMS
                message = client.messages.create(
                    from_=TWILIO_PHONE_NUMBER,
                    body=message,
                    to=recipient
                )
                
                results[f"sms_{recipient}"] = {
                    "status": "success",
                    "message_sid": message.sid
                }
                
                logger.info(f"SMS sent successfully to {recipient}. Message SID: {message.sid}")
                
            except Exception as e:
                logger.error(f"SMS sending error ({recipient}): {str(e)}")
                results[f"sms_{recipient}"] = {
                    "status": "error",
                    "error": str(e)
                }
        
        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Alert sending error: {str(e)}")
        return {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

# Test function
async def test_alert_sender():
    # Test alert data
    test_data = {
        "user_id": "test_user",
        "alert_type": "medication_not_taken",
        "message": "Hello, have you taken your medication today?",
        "recipients": ["+821012345678"]  # Test phone number
    }
    
    result = await send_alert(test_data)
    print(f"Alert sending result: {result}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_alert_sender())