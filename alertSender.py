import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from twilio.rest import Client

# 환경변수 로드
load_dotenv()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def send_alert(alert_data: dict) -> dict:
    """
    SMS 알림 전송
    
    Args:
        alert_data: 알림 데이터
            - user_id: 사용자 ID
            - alert_type: 알림 유형 (no_response, medication_not_taken)
            - message: 알림 메시지
            - recipients: 수신자 전화번호 목록
            
    Returns:
        dict: 알림 전송 결과
    """
    try:
        logger.info(f"알림 전송 시작: {alert_data.get('alert_type')}")
        
        message = alert_data.get("message")
        recipients = alert_data.get("recipients", [])
        
        # Twilio 클라이언트 초기화
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        
        # 결과 저장
        results = {}
        
        # SMS 전송
        for recipient in recipients:
            try:
                # SMS 전송
                message = client.messages.create(
                    from_=TWILIO_PHONE_NUMBER,
                    body=message,
                    to=recipient
                )
                
                results[f"sms_{recipient}"] = {
                    "status": "success",
                    "message_sid": message.sid
                }
                
                logger.info(f"SMS 전송 성공: {recipient}")
                
            except Exception as e:
                logger.error(f"SMS 전송 오류 ({recipient}): {str(e)}")
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
        logger.error(f"알림 전송 오류: {str(e)}")
        return {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

# 테스트 함수
async def test_alert_sender():
    # 테스트 알림 데이터
    test_data = {
        "user_id": "test_user",
        "alert_type": "medication_not_taken",
        "message": "안녕하세요. 오늘 약을 복용하셨나요?",
        "recipients": ["+821012345678"]  # 테스트용 전화번호
    }
    
    result = await send_alert(test_data)
    print(f"알림 전송 결과: {result}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_alert_sender())