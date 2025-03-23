import random
import asyncio
import logging
import threading
from managers.firebase_manager import FirebaseManager
from utils.data_fetcher import DataFetcher

from utils.projection_processor import ProjectionProcessor
from utils.signal_handler import SignalHandler
from utils.realtime_listener import RealtimeListener
from managers.redis_manager import RedisManager

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Shutdown event for graceful termination
shutdown_event = threading.Event()
#VPN Manager


data_fetcher = DataFetcher()
redis_manager = RedisManager()
firebase_manager = FirebaseManager("./config/serviceAccountKey.json", "https://sportbets-1e08a-default-rtdb.firebaseio.com/", redis_manager)
# Initialize the processor, signal handler, and real-time listener
projection_processor = ProjectionProcessor( redis_manager, firebase_manager, data_fetcher)
signal_handler = SignalHandler(shutdown_event)
realtime_listener = RealtimeListener(firebase_manager,redis_manager)

async def monitor_prizepicks():
    """
    Main monitoring function to detect changes in projections and process them.
    """
    logging.info("Loading player and PrizePicks NFL caches from Firebase.")
  
    logging.info("Caches loaded successfully.")

    loop_run_count = 0
    try:
        while not shutdown_event.is_set():
            loop_run_count += 1
            logging.info(f"Starting loop iteration {loop_run_count}...")

            # Fetch projections from PrizePicks API
            projections = await data_fetcher.fetch_projections(7)
            logging.info(len(projections))
            logging.info(f"This is the length of the PrizePicks Cache {redis_manager.count_projections()}")
            if projections:
                filtered_projections = projection_processor.filter_relevant_projections(projections)

                if filtered_projections:
                    logging.info(f"Processing {len(filtered_projections)} players with updated or new projections.")
                    remaining_projections = projection_processor.process_projections(filtered_projections)

                    # Check for remaining projections and process them
                    if remaining_projections:
                        logging.info(f"Fetching remaining player data for {len(remaining_projections)} players.")
                        await projection_processor.fetch_remaining_players(remaining_projections)
                    else:
                        logging.info("No remaining projections to fetch.")
                else:
                    logging.info("No relevant changes detected, skipping upload.")
            else:
                logging.warning("No projections fetched or an error occurred while fetching projections.")

            # Introduce a random delay between 30 and 45 seconds
            random_delay = random.randint(30, 45)
            logging.info(f"Sleeping for {random_delay} seconds before next iteration.")
            await asyncio.sleep(random_delay)

    except asyncio.CancelledError:
        logging.info("Monitoring task cancelled.")
    finally:
        logging.info("Cleaning up before exiting.")

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal_handler.register_signal_handlers()

    # Start the real-time listener for Firebase updates
    logging.info("Starting Realtime Database listener for NFL players...")
    realtime_listener.start()

    # Start monitoring PrizePicks projections
    try:
        asyncio.run(monitor_prizepicks())
    except KeyboardInterrupt:
        logging.info("Program terminated by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info("Program exited.")