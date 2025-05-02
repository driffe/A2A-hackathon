from datetime import datetime, timedelta
from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
import asyncio
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# 활동 함수 임포트
from voiceAgent import start_conversation
from alertSender import send_alert

# 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 사용자 데이터 저장소 (실제로는 데이터베이스 사용)
users = {}
medication_records = {}

# 워크플로우 매개변수 정의
class MedicationCheckParams:
    def __init__(self, user_id: str, schedule_time: str, frequency: str = "daily"):
        self.user_id = user_id
        self.schedule_time = schedule_time
        self.frequency = frequency
        self.consecutive_no_responses = 0
        self.max_no_responses = 3

# 활동 함수 정의
@activity.defn
async def execute_medication_check(user_id: str) -> dict:
    # 1. 음성 대화 시작
    conversation_result = await start_conversation(user_id)
    
    # 2. 응답이 없는 경우
    if not conversation_result or conversation_result.get("status") == "no_response":
        return {
            "status": "no_response",
            "timestamp": datetime.now().isoformat()
        }
    
    # 3. 약 복용 여부 확인
    structured_data = conversation_result.get("structured_data", {})
    medication_taken = structured_data.get("medication_taken", False)
    
    result = {
        "timestamp": datetime.now().isoformat(),
        "user_id": user_id,
        "medication_taken": medication_taken,
        "details": structured_data.get("details", "")
    }
    
    # 4. 약 복용 여부에 따른 알림 전송
    if not medication_taken:
        await send_alert({
            "user_id": user_id,
            "alert_type": "medication_not_taken",
            "message": f"사용자가 약을 복용하지 않았습니다. 상세 내용: {structured_data.get('details', '정보 없음')}",
            "severity": "medium"
        })
    
    return result

@activity.defn
async def send_no_response_alert(user_id: str, consecutive_no_responses: int):
    await send_alert({
        "user_id": user_id,
        "alert_type": "no_response",
        "message": f"{consecutive_no_responses}회 연속 응답이 없습니다. 약 복용 확인이 필요합니다.",
        "severity": "high"
    })

# 약 복용 체크 워크플로우 정의
@workflow.defn
class MedicationCheckWorkflow:
    def __init__(self):
        self.params = None
        self.last_check_time = None
        self.medication_history = []
    
    @workflow.run
    async def run(self, params: MedicationCheckParams) -> dict:
        self.params = params
        self.last_check_time = datetime.now()
        
        # 첫 번째 체크 실행
        result = await self._execute_check()
        
        # 주기적 체크 반복 설정
        if self.params.frequency == "daily":
            interval = timedelta(days=1)
        elif self.params.frequency == "weekly":
            interval = timedelta(weeks=1)
        elif self.params.frequency == "hourly":
            interval = timedelta(hours=1)
        else:
            interval = timedelta(days=1)  # 기본값
        
        # 다음 체크 예약
        next_check_time = self.last_check_time + interval
        workflow.create_timer(next_check_time).wait()
        await self._execute_check()
        
        return result
    
    async def _execute_check(self) -> dict:
        user_id = self.params.user_id
        
        # 약 복용 체크 실행
        result = await workflow.execute_activity(
            execute_medication_check,
            args=[user_id],
            start_to_close_timeout=timedelta(minutes=5)
        )
        
        # 응답이 없는 경우 처리
        if result.get("status") == "no_response":
            self.params.consecutive_no_responses += 1
            
            # 연속 무응답 임계값 초과 시 알림 전송
            if self.params.consecutive_no_responses >= self.params.max_no_responses:
                await workflow.execute_activity(
                    send_no_response_alert,
                    args=[user_id, self.params.consecutive_no_responses],
                    start_to_close_timeout=timedelta(minutes=1)
                )
        else:
            self.params.consecutive_no_responses = 0
            self.medication_history.append(result)
        
        return result

# Temporal 워커 실행 함수
async def run_worker():
    client = await Client.connect("localhost:7233")
    
    worker = Worker(
        client,
        task_queue="medication-check-queue",
        workflows=[MedicationCheckWorkflow],
        activities=[execute_medication_check, send_no_response_alert]
    )
    
    await worker.run()

# 워크플로우 시작 함수
async def start_workflow(user_id: str, schedule_time: str, frequency: str = "daily"):
    client = await Client.connect("localhost:7233")
    
    params = MedicationCheckParams(
        user_id=user_id,
        schedule_time=schedule_time,
        frequency=frequency
    )
    
    handle = await client.start_workflow(
        MedicationCheckWorkflow.run,
        args=[params],
        id=f"medication-check-{user_id}-{datetime.now().isoformat()}",
        task_queue="medication-check-queue"
    )
    
    return handle

class MedicationSchedule:
    def __init__(self, user_id: str, schedule_time: str, frequency: str = "daily"):
        self.user_id = user_id
        self.schedule_time = schedule_time
        self.frequency = frequency
        self.last_check = None
        self.status = "active"
        
    def to_dict(self):
        return {
            "user_id": self.user_id,
            "schedule_time": self.schedule_time,
            "frequency": self.frequency,
            "last_check": self.last_check,
            "status": self.status
        }

def create_schedule(user_id: str, schedule_time: str, frequency: str = "daily") -> dict:
    """새로운 약 복용 스케줄 생성"""
    try:
        if user_id not in users:
            return {"status": "error", "message": "사용자를 찾을 수 없습니다"}
            
        schedule = MedicationSchedule(user_id, schedule_time, frequency)
        users[user_id]["schedules"].append(schedule)
        
        # 스케줄러에 작업 추가
        add_schedule_to_scheduler(schedule)
        
        return {
            "status": "success",
            "message": "스케줄이 생성되었습니다",
            "schedule": schedule.to_dict()
        }
    except Exception as e:
        logger.error(f"스케줄 생성 오류: {str(e)}")
        return {"status": "error", "message": str(e)}

def add_schedule_to_scheduler(schedule: MedicationSchedule):
    """스케줄러에 작업 추가"""
    try:
        # 스케줄 시간 파싱
        hour, minute = map(int, schedule.schedule_time.split(":"))
        
        # 스케줄러에 작업 추가
        if schedule.frequency == "daily":
            scheduler.add_job(
                check_medication,
                CronTrigger(hour=hour, minute=minute),
                args=[schedule.user_id],
                id=f"medication_check_{schedule.user_id}"
            )
        elif schedule.frequency == "weekly":
            scheduler.add_job(
                check_medication,
                CronTrigger(day_of_week="mon-sun", hour=hour, minute=minute),
                args=[schedule.user_id],
                id=f"medication_check_{schedule.user_id}"
            )
            
        logger.info(f"스케줄 추가됨: {schedule.user_id} - {schedule.schedule_time}")
    except Exception as e:
        logger.error(f"스케줄러 작업 추가 오류: {str(e)}")

async def check_medication(user_id: str):
    """약 복용 체크 실행"""
    try:
        logger.info(f"약 복용 체크 시작: {user_id}")
        
        # SMS 전송
        from alertSender import send_alert
        
        message = "안녕하세요. 오늘 약을 복용하셨나요? (예/아니오로 답변해주세요)"
        
        result = await send_alert({
            "user_id": user_id,
            "message": message,
            "recipients": [users[user_id]["phone"]]
        })
        
        # 체크 기록 저장
        record = {
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "status": "sent",
            "message": message
        }
        
        if user_id not in medication_records:
            medication_records[user_id] = []
        medication_records[user_id].append(record)
        
        return result
        
    except Exception as e:
        logger.error(f"약 복용 체크 오류: {str(e)}")
        return {"status": "error", "message": str(e)}

def get_medication_chart(user_id: str) -> dict:
    """약 복용 차트 데이터 생성"""
    try:
        if user_id not in medication_records:
            return {"status": "error", "message": "기록이 없습니다"}
            
        # 데이터프레임 생성
        df = pd.DataFrame(medication_records[user_id])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # 일별 통계
        daily_stats = df.groupby(df["timestamp"].dt.date).size().reset_index(name="count")
        
        # 차트 생성
        fig = go.Figure()
        
        # 일별 알림 전송 횟수
        fig.add_trace(go.Bar(
            x=daily_stats["timestamp"],
            y=daily_stats["count"],
            name="알림 전송 횟수"
        ))
        
        # 차트 레이아웃 설정
        fig.update_layout(
            title="약 복용 알림 통계",
            xaxis_title="날짜",
            yaxis_title="알림 전송 횟수",
            showlegend=True
        )
        
        return {
            "status": "success",
            "chart": fig.to_json()
        }
        
    except Exception as e:
        logger.error(f"차트 생성 오류: {str(e)}")
        return {"status": "error", "message": str(e)}

# 스케줄러 초기화
scheduler = BackgroundScheduler()
scheduler.start()

# 테스트 함수
def test_schedule():
    # 테스트 사용자 생성
    test_user = {
        "user_id": "test_user",
        "name": "테스트 사용자",
        "phone": "+821012345678",
        "schedules": []
    }
    users["test_user"] = test_user
    
    # 테스트 스케줄 생성
    result = create_schedule("test_user", "09:00", "daily")
    print(f"스케줄 생성 결과: {result}")

def create_dummy_data():
    """더미 데이터 생성"""
    # 더미 사용자 생성
    dummy_users = [
        {
            "user_id": "user1",
            "name": "홍길동",
            "phone": "+14083048254",
            "schedules": []
        },
        {
            "user_id": "user2",
            "name": "김철수",
            "phone": "+821098765432",
            "schedules": []
        }
    ]
    
    # 더미 사용자 데이터 추가
    for user in dummy_users:
        users[user["user_id"]] = user
    
    # 더미 스케줄 생성
    for user in dummy_users:
        schedule = MedicationSchedule(user["user_id"], "09:00", "daily")
        users[user["user_id"]]["schedules"].append(schedule)
        add_schedule_to_scheduler(schedule)
    
    # 더미 약 복용 기록 생성
    for user in dummy_users:
        medication_records[user["user_id"]] = [
            {
                "timestamp": (datetime.now() - timedelta(days=i)).isoformat(),
                "user_id": user["user_id"],
                "medication_taken": i % 2 == 0,  # 번갈아가며 복용/미복용
                "details": "더미 데이터" if i % 2 == 0 else "약을 복용하지 않음"
            }
            for i in range(7)  # 최근 7일간의 기록
        ]
    
    logger.info("더미 데이터가 생성되었습니다")

if __name__ == "__main__":
    # 테스트용 워커 실행
    asyncio.run(run_worker())
    test_schedule()
    create_dummy_data()