# scheduler.py (DEMO MODE - Runs every 15 seconds)

import schedule
import time
import subprocess

def run_forecaster():
    """Triggers the demand_forecaster.py script."""
    print(f"[{time.ctime()}] SCHEDULER: Triggering the demand_forecaster.py script...")
    try:
        # We use subprocess.run to execute the script as a separate process.
        # This is a robust way to run it.
        result = subprocess.run(
            ["python", "demand_forecaster.py"], 
            check=True, 
            capture_output=True, # Capture the stdout and stderr
            text=True # Decode stdout/stderr as text
        )
        print(f"[{time.ctime()}] FORECASTER LOG:\n{result.stdout}")
        print(f"[{time.ctime()}] SCHEDULER: demand_forecaster.py finished successfully.")
    except subprocess.CalledProcessError as e:
        # This will catch errors if the script returns a non-zero exit code
        print(f"🔥 [{time.ctime()}] SCHEDULER: demand_forecaster.py failed!")
        print(f"RETURN CODE: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")

if __name__ == "__main__":
    print("🚀 Scheduler Service Started in DEMO MODE. Forecasting is scheduled for every 15 seconds.")
    
    # --- THIS IS THE KEY CHANGE ---
    # Schedule the job to run every 15 seconds
    schedule.every(15).seconds.do(run_forecaster)
    
    # We won't run it on startup, we'll let the schedule kick in.
    print("Waiting for the first scheduled run...")

    while True:
        schedule.run_pending()
        time.sleep(1) # Check for a pending job every second