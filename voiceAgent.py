import os
import requests
import logging
import json
import base64
import time
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
VAPI_API_KEY = os.getenv("VAPI_API_KEY")
VAPI_PHONE_NUMBER_ID = os.getenv("VAPI_PHONE_NUMBER_ID")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def start_conversation(user_id: str) -> dict:
    """
    Start a voice conversation with the user using VAPI.
    Args:
        user_id: User ID
    Returns:
        dict: Conversation result (including structured data)
    """
    try:
        logger.info(f"Starting voice conversation with user {user_id}")
        headers = {
            "Authorization": f"Bearer {VAPI_API_KEY}",
            "Content-Type": "application/json"
        }
        # Assistant configuration
        assistant_config = {
            "transcriber": {
                "provider": "deepgram",
                "language": "en"  # English
            },
            "model": {
                "provider": "anthropic",  # Using Claude
                "model": "claude-3-sonnet-20240229",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a kind and clear assistant who checks whether an elderly person has taken their medication. Keep the conversation short and simply confirm whether the user has taken their medication today. If the user has taken it, respond positively. If not, briefly mention the importance without making them feel pressured."
                    }
                ]
            },
            "voice": {
                "provider": "elevenlabs",
                "voiceId": "english_female_voice"
            },
            "firstMessage": "Hello, have you taken your medication today?",
            "silenceTimeoutSeconds": 10,  # End after 10 seconds of silence
            "analysisPlan": {
                "summaryPrompt": "Briefly summarize the conversation and whether the medication was taken.",
                "structuredDataPrompt": "Extract whether the user took their medication and any relevant details.",
                "structuredDataSchema": {
                    "type": "object",
                    "properties": {
                        "medication_taken": {
                            "type": "boolean",
                            "description": "Whether the user took their medication"
                        },
                        "details": {
                            "type": "string",
                            "description": "Details about medication intake (reason for not taking, time taken, etc.)"
                        },
                        "response_quality": {
                            "type": "string",
                            "enum": ["clear", "confused", "no_response"]
                        }
                    },
                    "required": ["medication_taken"]
                }
            }
        }
        # Create VAPI call
        payload = {
            "assistant": assistant_config,
            "name": f"Medication Check - {user_id} - {datetime.now().isoformat()}"
        }
        # Create call request
        call_response = requests.post(
            "https://api.vapi.ai/call/web",
            headers=headers,
            json=payload,
            timeout=60
        )
        # Validate response
        if call_response.status_code != 200:
            logger.error(f"VAPI call creation error: {call_response.status_code} - {call_response.text}")
            raise Exception(f"Call creation failed: {call_response.text}")
        # Extract call ID
        call_result = call_response.json()
        call_id = call_result.get("id")
        if not call_id:
            raise Exception("Call ID not found in VAPI response")
        logger.info(f"Call created successfully. Call ID: {call_id}")
        # Wait for call completion
        call_completed = await wait_for_call_completion(call_id, headers)
        if not call_completed:
            logger.warning("Call was not completed. Treating as no response.")
            return {
                "status": "no_response",
                "timestamp": datetime.now().isoformat(),
                "call_id": call_id
            }
        # Get call analysis result
        analysis_response = requests.get(
            f"https://api.vapi.ai/call/{call_id}/analysis",
            headers=headers,
            timeout=30
        )
        if analysis_response.status_code != 200:
            logger.error(f"Error fetching analysis result: {analysis_response.status_code}")
            analysis_result = {}
        else:
            analysis_result = analysis_response.json()
        # Compose result
        result = {
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "call_id": call_id,
            "summary": analysis_result.get("summary", ""),
            "structured_data": analysis_result.get("structuredData", {})
        }
        return result
    except Exception as e:
        logger.error(f"Voice conversation error: {str(e)}")
        return {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

async def wait_for_call_completion(call_id: str, headers: dict, max_retries: int = 20) -> bool:
    """
    Wait until the call is completed.
    Args:
        call_id: Call ID
        headers: Request headers
        max_retries: Maximum number of retries
    Returns:
        bool: Whether the call was completed
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Check call status
            response = requests.get(
                f"https://api.vapi.ai/call/{call_id}",
                headers=headers,
                timeout=30
            )
            if response.status_code == 200:
                call_status = response.json()
                status = call_status.get("status")
                if status in ["completed", "failed", "cancelled"]:
                    logger.info(f"Call completed. Status: {status}")
                    return status == "completed"
                logger.debug(f"Call in progress. Status: {status}")
            else:
                logger.error(f"Error checking call status: {response.status_code}")
            # Wait 5 seconds before retrying
            time.sleep(5)
            retry_count += 1
        except Exception as e:
            logger.error(f"Error while checking call status: {str(e)}")
            time.sleep(5)
            retry_count += 1
    logger.warning(f"Exceeded maximum retry count ({max_retries}).")
    return False

def call_user_and_record_result(to_phone_number, user_id):
    """
    VAPI using outbound call to user
    Args:
        to_phone_number: recipient phone number (converted to E.164 format)
        user_id: user ID
    Returns:
        dict: call result
    """
    # Check if phone number is in E.164 format
    if not to_phone_number.startswith('+'):
        to_phone_number = f"+{to_phone_number}"  # Convert to international format
    
    headers = {
        "Authorization": f"Bearer {VAPI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # VAPI outbound call request
    call_url = "https://api.vapi.ai/call"
    call_payload = {
        "assistantId": VAPI_ASSISTANT_ID,  # pre-created assistant ID
        "phoneNumberId": VAPI_PHONE_NUMBER_ID,  # outbound phone number ID
        "customer": {
            "number": to_phone_number  # recipient phone number
        }
    }
    
    try:
        logger.info(f"Attempting outbound call to {to_phone_number}")
        call_response = requests.post(
            call_url, 
            headers=headers, 
            json=call_payload
        )
        
        if call_response.status_code != 200:
            error_msg = f"Call creation failed: {call_response.text}"
            logger.error(error_msg)
            return {
                "status": "error",
                "error": "Call creation failed",
                "details": call_response.json()
            }
        
        call_data = call_response.json()
        logger.info(f"Call successfully initiated. Call ID: {call_data.get('id')}")
        
        return {
            "status": "success",
            "call_id": call_data.get("id"),
            "details": call_data
        }
        
    except Exception as e:
        error_msg = f"Error during call attempt: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "error": str(e)
        }

# Test function
async def test_voice_agent():
    result = await start_conversation("test_user")
    print(f"Voice conversation result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_voice_agent())