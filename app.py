import os
import asyncio
import logging
import click
import json
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler

# 워크플로우 및 활동 함수 임포트
from workflow import (
    start_workflow,
    run_worker,
    MedicationSchedule,
    add_schedule_to_scheduler,
    get_medication_chart,
    users,
    medication_records
)
from voiceAgent import start_conversation, call_user_and_record_result
from alertSender import send_alert

# 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask 앱 초기화
app = Flask(__name__)

# 현재 실행 중인 워커 프로세스
worker_process = None

# 사용자 데이터 (실제로는 데이터베이스 사용)
users = {}

# APScheduler 인스턴스 생성 및 시작
scheduler = BackgroundScheduler()
scheduler.start()

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

@app.route('/api/users', methods=['POST'])
def create_user():
    """새 사용자 등록"""
    data = request.json
    
    user_id = data.get('user_id')
    name = data.get('name')
    phone = data.get('phone')
    
    if not user_id or not name or not phone:
        return jsonify({
            "status": "error",
            "message": "필수 정보가 누락되었습니다 (user_id, name, phone)"
        }), 400
    
    # 사용자 정보 저장
    users[user_id] = {
        "user_id": user_id,
        "name": name,
        "phone": phone,
        "created_at": datetime.now().isoformat(),
        "schedules": []
    }
    
    return jsonify({
        "status": "success",
        "message": "사용자가 등록되었습니다",
        "user": users[user_id]
    })

@app.route('/api/schedules', methods=['POST'])
def create_schedule_route():
    """약 복용 체크 일정 생성"""
    data = request.json
    
    user_id = data.get('user_id')
    schedule_time = data.get('schedule_time')
    frequency = data.get('frequency', 'daily')
    
    if not user_id or not schedule_time:
        return jsonify({
            "status": "error",
            "message": "필수 정보가 누락되었습니다 (user_id, schedule_time)"
        }), 400
    
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"사용자 ID {user_id}를 찾을 수 없습니다"
        }), 404
    
    try:
        # 스케줄 생성
        schedule = MedicationSchedule(user_id, schedule_time, frequency)
        users[user_id]["schedules"].append(schedule)
        
        # 스케줄러에 작업 추가
        add_schedule_to_scheduler(schedule)
        
        return jsonify({
            "status": "success",
            "message": "스케줄이 생성되었습니다",
            "schedule": schedule.to_dict()
        })
    except Exception as e:
        logger.error(f"스케줄 생성 오류: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/check-now', methods=['POST'])
def check_now():
    """즉시 약 복용 체크 실행 (자동 전화)"""
    data = request.json
    user_id = data.get('user_id')
    
    if not user_id:
        return jsonify({
            "status": "error",
            "message": "사용자 ID가 필요합니다"
        }), 400
    
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"사용자 ID {user_id}를 찾을 수 없습니다"
        }), 404
    
    try:
        # 전화번호 가져오기
        to_phone_number = users[user_id]["phone"]
        # 자동 전화 실행
        call_result = call_user_and_record_result(to_phone_number, user_id)
        # 결과 저장 (예시)
        if user_id not in medication_records:
            medication_records[user_id] = []
        medication_records[user_id].append({
            "timestamp": datetime.now().isoformat(),
            "call_result": call_result
        })
        return jsonify({
            "status": "success",
            "message": "자동 전화가 실행되었습니다.",
            "call_result": call_result
        })
    except Exception as e:
        logger.error(f"자동 전화 오류: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"자동 전화 오류: {str(e)}"
        }), 500

@app.route('/api/chart/<user_id>')
def get_chart(user_id):
    """약 복용 차트 데이터 조회"""
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"사용자 ID {user_id}를 찾을 수 없습니다"
        }), 404
    
    result = get_medication_chart(user_id)
    
    if result["status"] == "error":
        return jsonify(result), 500
    
    return jsonify(result)

@app.route('/api/users/<user_id>')
def get_user(user_id):
    """사용자 정보 조회"""
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"사용자 ID {user_id}를 찾을 수 없습니다"
        }), 404
    
    return jsonify({
        "status": "success",
        "user": users[user_id]
    })

@app.route('/api/records/<user_id>')
def get_records(user_id):
    """약 복용 기록 조회"""
    if user_id not in medication_records:
        return jsonify({
            "status": "error",
            "message": "기록이 없습니다"
        }), 404
    
    return jsonify({
        "status": "success",
        "records": medication_records[user_id]
    })

@app.route('/api/init-dummy', methods=['POST'])
def init_dummy():
    """더미 데이터 초기화"""
    try:
        from workflow import create_dummy_data
        create_dummy_data()
        return jsonify({
            "status": "success",
            "message": "더미 데이터가 생성되었습니다"
        })
    except Exception as e:
        logger.error(f"더미 데이터 생성 오류: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/call-now', methods=['POST'])
def call_now():
    """즉시 전화걸기"""
    data = request.json
    user_id = data.get('user_id')
    if not user_id or user_id not in users:
        return jsonify({"status": "error", "message": "유효하지 않은 사용자 ID"}), 400
    to_phone_number = users[user_id]["phone"]
    try:
        call_result = call_user_and_record_result(to_phone_number, user_id)
        if user_id not in medication_records:
            medication_records[user_id] = []
        medication_records[user_id].append({
            "timestamp": datetime.now().isoformat(),
            "call_result": call_result
        })
        return jsonify({"status": "success", "message": "즉시 전화가 실행되었습니다.", "call_result": call_result})
    except Exception as e:
        logger.error(f"즉시 전화 오류: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/schedule-call', methods=['POST'])
def schedule_call():
    """예약 전화걸기"""
    data = request.json
    user_id = data.get('user_id')
    schedule_time = data.get('schedule_time')  # ISO 포맷 문자열
    if not user_id or user_id not in users or not schedule_time:
        return jsonify({"status": "error", "message": "필수 정보가 누락되었습니다."}), 400
    to_phone_number = users[user_id]["phone"]
    try:
        # 예약 작업 등록
        def job():
            call_result = call_user_and_record_result(to_phone_number, user_id)
            if user_id not in medication_records:
                medication_records[user_id] = []
            medication_records[user_id].append({
                "timestamp": datetime.now().isoformat(),
                "call_result": call_result
            })
        run_time = datetime.fromisoformat(schedule_time)
        scheduler.add_job(job, 'date', run_date=run_time)
        return jsonify({"status": "success", "message": f"{schedule_time}에 전화가 예약되었습니다."})
    except Exception as e:
        logger.error(f"예약 전화 오류: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/users', methods=['GET'])
def get_users():
    """전체 사용자 목록 반환 (스케줄 객체는 dict로 변환)"""
    def serialize_user(user):
        user_copy = user.copy()
        user_copy["schedules"] = [
            s.to_dict() if hasattr(s, "to_dict") else s
            for s in user_copy.get("schedules", [])
        ]
        return user_copy
    return jsonify({"users": [serialize_user(u) for u in users.values()]})

def run_worker_process():
    """워커 프로세스에서 실행될 함수"""
    asyncio.run(run_worker())

def start_worker_process():
    """Temporal 워커 프로세스 시작"""
    global worker_process
    
    if worker_process is None or not worker_process.is_alive():
        import multiprocessing
        worker_process = multiprocessing.Process(target=run_worker_process)
        worker_process.start()
        logger.info(f"Temporal 워커 시작됨 (PID: {worker_process.pid})")

@click.group()
def cli():
    """약 복용 체크 시스템 CLI"""
    pass

@cli.command()
@click.option('--host', default='0.0.0.0', help='서버 호스트')
@click.option('--port', default=5001, help='서버 포트')
def serve(host, port):
    """웹 서버 실행"""
    # Flask 앱 실행
    app.run(host=host, port=port, debug=True)

@cli.command()
@click.argument('user_id')
@click.argument('schedule_time')
@click.option('--frequency', default='daily', help='체크 주기 (daily, weekly, hourly)')
def schedule(user_id, schedule_time, frequency):
    """약 복용 체크 일정 생성"""
    async def run():
        try:
            workflow_handle = await start_workflow(user_id, schedule_time, frequency)
            print(f"약 복용 체크 일정이 생성되었습니다. 워크플로우 ID: {workflow_handle.id}")
        except Exception as e:
            print(f"오류: {str(e)}")
    
    # Temporal 워커 시작
    start_worker_process()
    
    # 일정 생성 실행
    asyncio.run(run())

@cli.command()
@click.argument('user_id')
def check(user_id):
    """즉시 약 복용 체크 실행"""
    async def run():
        try:
            # 음성 대화 시작
            print("음성 대화 시작...")
            conversation_result = await start_conversation(user_id)
            
            # 응답이 없는 경우
            if conversation_result.get("status") == "no_response":
                print("사용자가 응답하지 않았습니다.")
                await send_alert({
                    "user_id": user_id,
                    "alert_type": "no_response",
                    "message": "사용자가 약 복용 체크에 응답하지 않았습니다.",
                    "severity": "medium"
                })
                return
            
            # 약 복용 여부 확인
            structured_data = conversation_result.get("structured_data", {})
            medication_taken = structured_data.get("medication_taken", False)
            details = structured_data.get("details", "정보 없음")
            
            print(f"약 복용 여부: {'복용함' if medication_taken else '복용하지 않음'}")
            print(f"상세 정보: {details}")
            
            # 약 복용 여부에 따른 알림 전송
            if not medication_taken:
                print("미복용 알림 전송 중...")
                await send_alert({
                    "user_id": user_id,
                    "alert_type": "medication_not_taken",
                    "message": f"사용자가 약을 복용하지 않았습니다. 상세 내용: {details}",
                    "severity": "medium"
                })
                
        except Exception as e:
            print(f"약 복용 체크 오류: {str(e)}")
    
    # 약 복용 체크 실행
    asyncio.run(run())

@cli.command()
def worker():
    """Temporal 워커 실행"""
    print("Temporal 워커 시작 중...")
    asyncio.run(run_worker())

if __name__ == '__main__':
    cli()