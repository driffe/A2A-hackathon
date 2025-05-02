import os
import requests
import logging
import json
import base64
import time
from datetime import datetime
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()
VAPI_API_KEY = os.getenv("VAPI_API_KEY")
VAPI_PHONE_NUMBER_ID = os.getenv("VAPI_PHONE_NUMBER_ID")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def start_conversation(user_id: str) -> dict:
    """
    VAPI를 사용하여 사용자와의 음성 대화 시작
    
    Args:
        user_id: 사용자 ID
        
    Returns:
        dict: 대화 결과 (구조화된 데이터 포함)
    """
    try:
        logger.info(f"사용자 {user_id}와의 음성 대화 시작")
        
        headers = {
            "Authorization": f"Bearer {VAPI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # 어시스턴트 구성
        assistant_config = {
            "transcriber": {
                "provider": "deepgram",
                "language": "ko"  # 한국어
            },
            "model": {
                "provider": "anthropic",  # Claude 사용
                "model": "claude-3-sonnet-20240229",
                "messages": [
                    {
                        "role": "system",
                        "content": "당신은 노인의 약 복용 여부를 확인하는 친절하고 명확한 어시스턴트입니다. 대화는 짧게 유지하고, 오늘 약을 복용했는지 여부만 간단히 확인하세요. 사용자가 복용했다고 하면 긍정적으로 반응하고, 복용하지 않았다면 중요성을 간단히 언급하되 부담을 주지 않도록 하세요."
                    }
                ]
            },
            "voice": {
                "provider": "elevenlabs",
                "voiceId": "korean_female_voice"
            },
            "firstMessage": "안녕하세요, 오늘 약은 복용하셨나요?",
            "silenceTimeoutSeconds": 10,  # 10초 침묵 후 종료
            "analysisPlan": {
                "summaryPrompt": "대화 내용과 약 복용 여부를 간단히 요약하세요.",
                "structuredDataPrompt": "사용자가 약을 복용했는지 여부와 관련 세부 사항을 추출하세요.",
                "structuredDataSchema": {
                    "type": "object",
                    "properties": {
                        "medication_taken": {
                            "type": "boolean",
                            "description": "사용자가 약을 복용했는지 여부"
                        },
                        "details": {
                            "type": "string",
                            "description": "약 복용 관련 세부 정보 (미복용 이유, 복용 시간 등)"
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
        
        # VAPI 통화 생성
        payload = {
            "assistant": assistant_config,
            "name": f"약 복용 체크 - {user_id} - {datetime.now().isoformat()}"
        }
        
        # 통화 생성 요청
        call_response = requests.post(
            "https://api.vapi.ai/call/web",
            headers=headers,
            json=payload,
            timeout=60
        )
        
        # 응답 검증
        if call_response.status_code != 200:
            logger.error(f"VAPI 통화 생성 오류: {call_response.status_code} - {call_response.text}")
            raise Exception(f"통화 생성 실패: {call_response.text}")
        
        # 통화 ID 추출
        call_result = call_response.json()
        call_id = call_result.get("id")
        
        if not call_id:
            raise Exception("VAPI 응답에서 통화 ID를 찾을 수 없습니다")
        
        logger.info(f"통화 생성 성공. 통화 ID: {call_id}")
        
        # 통화 완료 대기
        call_completed = await wait_for_call_completion(call_id, headers)
        
        if not call_completed:
            logger.warning("통화가 완료되지 않았습니다. 무응답으로 처리합니다.")
            return {
                "status": "no_response",
                "timestamp": datetime.now().isoformat(),
                "call_id": call_id
            }
        
        # 통화 분석 결과 가져오기
        analysis_response = requests.get(
            f"https://api.vapi.ai/call/{call_id}/analysis",
            headers=headers,
            timeout=30
        )
        
        if analysis_response.status_code != 200:
            logger.error(f"분석 결과 가져오기 오류: {analysis_response.status_code}")
            analysis_result = {}
        else:
            analysis_result = analysis_response.json()
        
        # 결과 구성
        result = {
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "call_id": call_id,
            "summary": analysis_result.get("summary", ""),
            "structured_data": analysis_result.get("structuredData", {})
        }
        
        return result
        
    except Exception as e:
        logger.error(f"음성 대화 오류: {str(e)}")
        return {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

async def wait_for_call_completion(call_id: str, headers: dict, max_retries: int = 20) -> bool:
    """
    통화가 완료될 때까지 대기
    
    Args:
        call_id: 통화 ID
        headers: 요청 헤더
        max_retries: 최대 재시도 횟수
        
    Returns:
        bool: 통화 완료 여부
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 통화 상태 확인
            response = requests.get(
                f"https://api.vapi.ai/call/{call_id}",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                call_status = response.json()
                status = call_status.get("status")
                
                if status in ["completed", "failed", "cancelled"]:
                    logger.info(f"통화 완료. 상태: {status}")
                    return status == "completed"
                
                logger.debug(f"통화 진행 중. 상태: {status}")
            else:
                logger.error(f"통화 상태 확인 오류: {response.status_code}")
                
            # 5초 대기 후 재시도
            time.sleep(5)
            retry_count += 1
            
        except Exception as e:
            logger.error(f"통화 상태 확인 중 오류: {str(e)}")
            time.sleep(5)
            retry_count += 1
    
    logger.warning(f"최대 재시도 횟수({max_retries})를 초과했습니다.")
    return False

def call_user_and_record_result(to_phone_number, user_id):
    """
    VAPI Outbound Call API를 사용하여 지정된 번호로 전화를 걸고, 결과를 저장합니다.
    """
    url = "https://api.vapi.ai/call"
    headers = {
        "Authorization": f"Bearer {VAPI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "phoneNumberId": VAPI_PHONE_NUMBER_ID,
        "assistantId": VAPI_ASSISTANT_ID,
        "phoneNumber": {
            "twilioPhoneNumber": TWILIO_PHONE_NUMBER,
            "twilioAccountSid": TWILIO_ACCOUNT_SID
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    result = response.json()
    # 결과 저장 로직 (예: DB 또는 메모리)
    # 예시: medication_records[user_id].append({"timestamp": datetime.now().isoformat(), "call_result": result})
    return result

# 테스트 함수
async def test_voice_agent():
    result = await start_conversation("test_user")
    print(f"음성 대화 결과: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_voice_agent())