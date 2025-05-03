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

# Import activity functions
from voiceAgent import start_conversation
from alertSender import send_alert

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data storage (should use database in production)
users = {}
medication_records = {}

# Workflow parameters definition
class MedicationCheckParams:
    def __init__(self, user_id: str, schedule_time: str, frequency: str = "daily"):
        self.user_id = user_id
        self.schedule_time = schedule_time
        self.frequency = frequency
        self.consecutive_no_responses = 0
        self.max_no_responses = 3

# Activity function definition
@activity.defn
async def execute_medication_check(user_id: str) -> dict:
    # 1. Start voice conversation
    conversation_result = await start_conversation(user_id)
    
    # 2. Handle no response case
    if not conversation_result or conversation_result.get("status") == "no_response":
        # Send SMS for no response
        await send_alert({
            "user_id": user_id,
            "alert_type": "no_response",
            "message": "Call attempt result: No response. We will contact you again.",
            "recipients": [users[user_id]["sms_phone"]]
        })
        return {
            "status": "no_response",
            "timestamp": datetime.now().isoformat()
        }
    
    # 3. Check medication status
    structured_data = conversation_result.get("structured_data", {})
    medication_taken = structured_data.get("medication_taken", False)
    
    result = {
        "timestamp": datetime.now().isoformat(),
        "user_id": user_id,
        "medication_taken": medication_taken,
        "details": structured_data.get("details", "")
    }
    
    # 4. Send SMS summary of the call
    summary_message = f"Call result summary:\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
    summary_message += "Medication taken: " + ("Yes" if medication_taken else "No") + "\n"
    if result["details"]:
        summary_message += f"Details: {result['details']}"
    
    await send_alert({
        "user_id": user_id,
        "alert_type": "call_summary",
        "message": summary_message,
        "recipients": [users[user_id]["sms_phone"]]
    })
    
    # 5. Send alert based on medication status
    if not medication_taken:
        await send_alert({
            "user_id": user_id,
            "alert_type": "medication_not_taken",
            "message": f"You did not take your medication. Details: {structured_data.get('details', 'No information')}",
            "severity": "medium",
            "recipients": [users[user_id]["sms_phone"]]
        })
    
    return result

@activity.defn
async def send_no_response_alert(user_id: str, consecutive_no_responses: int):
    await send_alert({
        "user_id": user_id,
        "alert_type": "no_response",
        "message": f"{consecutive_no_responses} consecutive responses have not been received. Medication check is needed.",
        "severity": "high"
    })

# Medication check workflow definition
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
        
        # Execute first check
        result = await self._execute_check()
        
        # Set periodic check repetition
        if self.params.frequency == "daily":
            interval = timedelta(days=1)
        elif self.params.frequency == "weekly":
            interval = timedelta(weeks=1)
        elif self.params.frequency == "hourly":
            interval = timedelta(hours=1)
        else:
            interval = timedelta(days=1)  # Default value
        
        # Schedule next check
        next_check_time = self.last_check_time + interval
        workflow.create_timer(next_check_time).wait()
        await self._execute_check()
        
        return result
    
    async def _execute_check(self) -> dict:
        user_id = self.params.user_id
        
        # Execute medication check
        result = await workflow.execute_activity(
            execute_medication_check,
            args=[user_id],
            start_to_close_timeout=timedelta(minutes=5)
        )
        
        # Handle no response case
        if result.get("status") == "no_response":
            self.params.consecutive_no_responses += 1
            
            # Send alert if consecutive no response threshold is exceeded
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

# Temporal worker execution function
async def run_worker():
    client = await Client.connect("localhost:7233")
    
    worker = Worker(
        client,
        task_queue="medication-check-queue",
        workflows=[MedicationCheckWorkflow],
        activities=[execute_medication_check, send_no_response_alert]
    )
    
    await worker.run()

# Workflow start function
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
    """Create new medication schedule"""
    try:
        if user_id not in users:
            return {"status": "error", "message": "User not found"}
            
        schedule = MedicationSchedule(user_id, schedule_time, frequency)
        users[user_id]["schedules"].append(schedule)
        
        # Add job to scheduler
        add_schedule_to_scheduler(schedule)
        
        return {
            "status": "success",
            "message": "Schedule created successfully",
            "schedule": schedule.to_dict()
        }
    except Exception as e:
        logger.error(f"Schedule creation error: {str(e)}")
        return {"status": "error", "message": str(e)}

def add_schedule_to_scheduler(schedule: MedicationSchedule):
    """Add job to scheduler"""
    try:
        # Parse schedule time
        hour, minute = map(int, schedule.schedule_time.split(":"))
        
        # Add job to scheduler
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
            
        logger.info(f"Schedule added: {schedule.user_id} - {schedule.schedule_time}")
    except Exception as e:
        logger.error(f"Scheduler job addition error: {str(e)}")

async def check_medication(user_id: str):
    """Execute medication check"""
    try:
        logger.info(f"Medication check started: {user_id}")
        
        # SMS sending
        from alertSender import send_alert
        
        message = "Hello. Have you taken your medication today? (Please respond with 'yes' or 'no')"
        
        result = await send_alert({
            "user_id": user_id,
            "message": message,
            "recipients": [users[user_id]["sms_phone"]]
        })
        
        # Check record storage
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
        logger.error(f"Medication check error: {str(e)}")
        return {"status": "error", "message": str(e)}

def get_medication_chart(user_id: str) -> dict:
    """Create medication chart data"""
    try:
        if user_id not in medication_records:
            return {"status": "error", "message": "No records found"}
            
        # Create dataframe
        df = pd.DataFrame(medication_records[user_id])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Daily statistics
        daily_stats = df.groupby(df["timestamp"].dt.date).size().reset_index(name="count")
        
        # Chart creation
        fig = go.Figure()
        
        # Daily alert sending count
        fig.add_trace(go.Bar(
            x=daily_stats["timestamp"],
            y=daily_stats["count"],
            name="Alert Sending Count"
        ))
        
        # Chart layout setting
        fig.update_layout(
            title="Medication Alert Sending Statistics",
            xaxis_title="Date",
            yaxis_title="Alert Sending Count",
            showlegend=True
        )
        
        return {
            "status": "success",
            "chart": fig.to_json()
        }
        
    except Exception as e:
        logger.error(f"Chart creation error: {str(e)}")
        return {"status": "error", "message": str(e)}

# Scheduler initialization
scheduler = BackgroundScheduler()
scheduler.start()

# Test function
def test_schedule():
    # Create test user
    test_user = {
        "user_id": "test_user",
        "name": "Test User",
        "phone": "+821012345678",
        "sms_phone": "+821012345678",
        "schedules": []
    }
    users["test_user"] = test_user
    
    # Create test schedule
    result = create_schedule("test_user", "09:00", "daily")
    print(f"Schedule creation result: {result}")

def create_dummy_data():
    """Create dummy data"""
    # Create dummy users
    dummy_users = [
        {
            "user_id": "user1",
            "name": "John Doe",
            "phone": "+14083048254",
            "sms_phone": "+14083048254",
            "schedules": []
        },
        {
            "user_id": "user2",
            "name": "Jane Smith",
            "phone": "+821098765432",
            "sms_phone": "+821098765432",
            "schedules": []
        }
    ]
    
    # Add dummy user data
    for user in dummy_users:
        users[user["user_id"]] = user
    
    # Create dummy schedules
    for user in dummy_users:
        schedule = MedicationSchedule(user["user_id"], "09:00", "daily")
        users[user["user_id"]]["schedules"].append(schedule)
        add_schedule_to_scheduler(schedule)
    
    # Create dummy medication records
    for user in dummy_users:
        medication_records[user["user_id"]] = [
            {
                "timestamp": (datetime.now() - timedelta(days=i)).isoformat(),
                "user_id": user["user_id"],
                "medication_taken": i % 2 == 0,  # Alternating between taken/not taken
                "details": "Dummy data" if i % 2 == 0 else "Medication not taken"
            }
            for i in range(7)  # Last 7 days of records
        ]
    
    logger.info("Dummy data has been created")

if __name__ == "__main__":
    # Run test worker
    asyncio.run(run_worker())
    test_schedule()
    create_dummy_data()