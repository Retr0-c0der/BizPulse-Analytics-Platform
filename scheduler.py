# scheduler.py (Robust Looping Version)
import time
import subprocess
import logging

# Configure logging to show timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_forecaster():
    """Triggers the demand_forecaster.py script with detailed logging."""
    logger.info("SCHEDULER: Triggering the demand_forecaster.py script...")
    try:
        # We use subprocess.run to execute the script as a separate process.
        result = subprocess.run(
            ["python", "demand_forecaster.py"],
            check=True,          # Raise an exception if the script returns a non-zero exit code
            capture_output=True, # Capture stdout and stderr
            text=True            # Decode stdout/stderr as text
        )
        # If the script succeeds, log its output
        logger.info(f"--- FORECASTER STDOUT ---\n{result.stdout}")
        if result.stderr:
            logger.warning(f"--- FORECASTER STDERR ---\n{result.stderr}")
        logger.info("SCHEDULER: demand_forecaster.py finished successfully.")

    except subprocess.CalledProcessError as e:
        # If the script fails (returns a non-zero exit code), log everything
        logger.error("🔥 SCHEDULER: demand_forecaster.py FAILED! 🔥")
        logger.error(f"RETURN CODE: {e.returncode}")
        logger.error(f"--- FAILED STDOUT ---\n{e.stdout}")
        logger.error(f"--- FAILED STDERR ---\n{e.stderr}")

    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"An unexpected error occurred while running the forecaster: {e}", exc_info=True)


if __name__ == "__main__":
    # Remove the old cronjob file if it exists, to avoid confusion
    try:
        import os
        if os.path.exists("cronjob"):
            os.remove("cronjob")
    except:
        pass

    logger.info("🚀 Scheduler Service Started. DEMO MODE: Forecasting will run every 30 seconds.")
    
    # We will run it once on startup to get initial data quickly
    run_forecaster()

    # Then loop forever, running it at a set interval
    while True:
        time.sleep(30) # Wait for 30 seconds
        run_forecaster()