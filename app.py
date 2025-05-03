import os
import asyncio
import logging
import click
import json
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler

# Import workflow and activity functions
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

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Current worker process
worker_process = None

# User data (should use database in production)
users = {}

# Initialize and start APScheduler
scheduler = BackgroundScheduler()
scheduler.start()

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

@app.route('/api/users', methods=['POST'])
def create_user():
    """Create new user"""
    data = request.json
    
    user_id = data.get('user_id')
    name = data.get('name')
    phone = data.get('phone')
    sms_phone = data.get('sms_phone', phone)  # 기본값은 phone과 동일
    
    if not user_id or not name or not phone:
        return jsonify({
            "status": "error",
            "message": "Missing required information (user_id, name, phone)"
        }), 400
    
    # Save user information
    users[user_id] = {
        "user_id": user_id,
        "name": name,
        "phone": phone,
        "sms_phone": sms_phone,  # 문자 수신 번호 추가
        "created_at": datetime.now().isoformat(),
        "schedules": []
    }
    
    return jsonify({
        "status": "success",
        "message": "User has been registered",
        "user": users[user_id]
    })

@app.route('/api/schedules', methods=['POST'])
def create_schedule_route():
    """Create medication check schedule"""
    data = request.json
    
    user_id = data.get('user_id')
    schedule_time = data.get('schedule_time')
    frequency = data.get('frequency', 'daily')
    
    if not user_id or not schedule_time:
        return jsonify({
            "status": "error",
            "message": "Missing required information (user_id, schedule_time)"
        }), 400
    
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"User ID {user_id} not found"
        }), 404
    
    try:
        # Create schedule
        schedule = MedicationSchedule(user_id, schedule_time, frequency)
        users[user_id]["schedules"].append(schedule)
        
        # Add job to scheduler
        add_schedule_to_scheduler(schedule)
        
        return jsonify({
            "status": "success",
            "message": "Schedule has been created",
            "schedule": schedule.to_dict()
        })
    except Exception as e:
        logger.error(f"Schedule creation error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/check-now', methods=['POST'])
def check_now():
    """Execute immediate medication check (automated call)"""
    data = request.json
    user_id = data.get('user_id')
    
    if not user_id:
        return jsonify({
            "status": "error",
            "message": "User ID is required"
        }), 400
    
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"User ID {user_id} not found"
        }), 404
    
    try:
        # Get phone number
        to_phone_number = users[user_id]["phone"]
        # Execute automated call
        call_result = call_user_and_record_result(to_phone_number, user_id)
        # Save result
        if user_id not in medication_records:
            medication_records[user_id] = []
        medication_records[user_id].append({
            "timestamp": datetime.now().isoformat(),
            "call_result": call_result
        })
        return jsonify({
            "status": "success",
            "message": "Automated call has been executed",
            "call_result": call_result
        })
    except Exception as e:
        logger.error(f"Automated call error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Automated call error: {str(e)}"
        }), 500

@app.route('/api/chart/<user_id>')
def get_chart(user_id):
    """Get medication chart data"""
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"User ID {user_id} not found"
        }), 404
    
    result = get_medication_chart(user_id)
    
    if result["status"] == "error":
        return jsonify(result), 500
    
    return jsonify(result)

@app.route('/api/users/<user_id>')
def get_user(user_id):
    """Get user information"""
    if user_id not in users:
        return jsonify({
            "status": "error",
            "message": f"User ID {user_id} not found"
        }), 404
    
    return jsonify({
        "status": "success",
        "user": users[user_id]
    })

@app.route('/api/records/<user_id>')
def get_records(user_id):
    """Get medication record data"""
    if user_id not in medication_records:
        return jsonify({
            "status": "error",
            "message": "No records found"
        }), 404
    
    return jsonify({
        "status": "success",
        "records": medication_records[user_id]
    })

@app.route('/api/init-dummy', methods=['POST'])
def init_dummy():
    """Initialize dummy data"""
    try:
        from workflow import create_dummy_data
        create_dummy_data()
        return jsonify({
            "status": "success",
            "message": "Dummy data has been created"
        })
    except Exception as e:
        logger.error(f"Error creating dummy data: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/call-now', methods=['POST'])
def call_now():
    """Make immediate call"""
    data = request.json
    user_id = data.get('user_id')
    if not user_id or user_id not in users:
        return jsonify({"status": "error", "message": "Invalid user ID"}), 400
    to_phone_number = users[user_id]["phone"]
    try:
        call_result = call_user_and_record_result(to_phone_number, user_id)
        if user_id not in medication_records:
            medication_records[user_id] = []
        medication_records[user_id].append({
            "timestamp": datetime.now().isoformat(),
            "call_result": call_result
        })
        return jsonify({"status": "success", "message": "Call has been initiated", "call_result": call_result})
    except Exception as e:
        logger.error(f"Call error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/schedule-call', methods=['POST'])
def schedule_call():
    """Schedule a call"""
    data = request.json
    user_id = data.get('user_id')
    schedule_time = data.get('schedule_time')  # ISO format string
    if not user_id or user_id not in users or not schedule_time:
        return jsonify({"status": "error", "message": "Missing required information"}), 400
    to_phone_number = users[user_id]["phone"]
    try:
        # Register scheduled job
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
        return jsonify({"status": "success", "message": f"Call scheduled for {schedule_time}"})
    except Exception as e:
        logger.error(f"Schedule call error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/users', methods=['GET'])
def get_users():
    """Return all users list (convert schedule objects to dict)"""
    def serialize_user(user):
        user_copy = user.copy()
        user_copy["schedules"] = [
            s.to_dict() if hasattr(s, "to_dict") else s
            for s in user_copy.get("schedules", [])
        ]
        return user_copy
    return jsonify({"users": [serialize_user(u) for u in users.values()]})

def run_worker_process():
    """Function to run in worker process"""
    asyncio.run(run_worker())

def start_worker_process():
    """Start Temporal worker process"""
    global worker_process
    
    if worker_process is None or not worker_process.is_alive():
        import multiprocessing
        worker_process = multiprocessing.Process(target=run_worker_process)
        worker_process.start()
        logger.info(f"Temporal worker started (PID: {worker_process.pid})")

@click.group()
def cli():
    """Medication Check System CLI"""
    pass

@cli.command()
@click.option('--host', default='0.0.0.0', help='Server host')
@click.option('--port', default=5001, help='Server port')
def serve(host, port):
    """Run web server"""
    # Run Flask app
    app.run(host=host, port=port, debug=True)

@cli.command()
@click.argument('user_id')
@click.argument('schedule_time')
@click.option('--frequency', default='daily', help='Check frequency (daily, weekly, hourly)')
def schedule(user_id, schedule_time, frequency):
    """Create medication check schedule"""
    async def run():
        try:
            workflow_handle = await start_workflow(user_id, schedule_time, frequency)
            print(f"Medication check schedule created. Workflow ID: {workflow_handle.id}")
        except Exception as e:
            print(f"Error: {str(e)}")
    
    # Start Temporal worker
    start_worker_process()
    
    # Execute schedule creation
    asyncio.run(run())

@cli.command()
@click.argument('user_id')
def check(user_id):
    """Execute immediate medication check"""
    async def run():
        try:
            # Start voice conversation
            print("Starting voice conversation...")
            conversation_result = await start_conversation(user_id)
            
            # No response case
            if conversation_result.get("status") == "no_response":
                print("User did not respond")
                await send_alert({
                    "user_id": user_id,
                    "alert_type": "no_response",
                    "message": "User did not respond to medication check",
                    "severity": "medium"
                })
                return
            
            # Check medication status
            structured_data = conversation_result.get("structured_data", {})
            medication_taken = structured_data.get("medication_taken", False)
            details = structured_data.get("details", "No information")
            
            print(f"Medication taken: {'Yes' if medication_taken else 'No'}")
            print(f"Details: {details}")
            
            # Send alert based on medication status
            if not medication_taken:
                print("Sending non-compliance alert...")
                await send_alert({
                    "user_id": user_id,
                    "alert_type": "medication_not_taken",
                    "message": f"User has not taken medication. Details: {details}",
                    "severity": "medium"
                })
                
        except Exception as e:
            print(f"Medication check error: {str(e)}")
    
    # Execute medication check
    asyncio.run(run())

@cli.command()
def worker():
    """Run Temporal worker"""
    print("Starting Temporal worker...")
    asyncio.run(run_worker())

if __name__ == '__main__':
    cli()