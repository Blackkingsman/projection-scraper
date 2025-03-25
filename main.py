import random
import asyncio
import logging
import threading

from managers.firebase_manager import FirebaseManager
from managers.firestore_manager import FirestoreManager
from managers.redis_manager import RedisManager
from utils.data_fetcher import DataFetcher
from utils.projection_processor import ProjectionProcessor
from utils.signal_handler import SignalHandler
from utils.realtime_listener import RealtimeListener

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Shutdown event for graceful termination
shutdown_event = threading.Event()

# Initialize managers and helpers
data_fetcher = DataFetcher()
redis_manager = RedisManager()

firebase_manager = FirebaseManager(
    "./config/serviceAccountKey.json",
    "https://sportbets-1e08a-default-rtdb.firebaseio.com/",
    redis_manager
)

firestore_manager = FirestoreManager("./config/serviceAccountKey.json")

projection_processor = ProjectionProcessor(redis_manager, firebase_manager, data_fetcher)
signal_handler = SignalHandler(shutdown_event)
realtime_listener = RealtimeListener(firebase_manager, redis_manager)

# Dictionary to keep track of running sports monitoring tasks
sport_tasks = {}

# Dispatcher callback invoked by Firestore listener
def dispatcher_callback(sport_name: str, active: bool, league_id: str, loop: asyncio.AbstractEventLoop):
    logging.info(f"Dispatcher received update: {sport_name} (League ID: {league_id}) is now {'active' if active else 'inactive'}.")

    if active:
        if sport_name not in sport_tasks or sport_tasks[sport_name].done():
            logging.info(f"Starting monitoring task for {sport_name}.")
            future = asyncio.run_coroutine_threadsafe(monitor_sport(sport_name, league_id), loop)
            sport_tasks[sport_name] = future
        else:
            logging.info(f"Monitoring task for {sport_name} is already running.")
    else:
        future = sport_tasks.get(sport_name)
        if future and not future.done():
            logging.info(f"Stopping monitoring task for {sport_name}.")
            future.cancel()

# Generalized monitoring coroutine per sport
async def monitor_sport(sport_name: str, league_id: str):
    logging.info(f"Monitor for {sport_name} started with League ID {league_id}.")

    try:
        while not shutdown_event.is_set():
            try:
                projections = await data_fetcher.fetch_projections(league_id, "prizepicks")
            except Exception as e:
                logging.error(f"Error fetching projections for {sport_name}: {e}")
                projections = None

            if projections:
                filtered_projections = projection_processor.filter_relevant_projections(projections)

                if filtered_projections:
                    logging.info(f"Processing {len(filtered_projections)} projections for {sport_name}.")
                    remaining_projections = projection_processor.process_projections(filtered_projections)

                    if remaining_projections:
                        logging.info(f"Fetching additional data for {len(remaining_projections)} players.")
                        await projection_processor.fetch_remaining_players(remaining_projections)
                    else:
                        logging.info("No additional projections to fetch.")
                else:
                    logging.info("No relevant changes detected.")
            else:
                logging.warning("No projections fetched or an error occurred while fetching projections.")

            await asyncio.sleep(random.randint(30, 45))

    except asyncio.CancelledError:
        logging.info(f"Monitoring for {sport_name} cancelled gracefully.")
    finally:
        logging.info(f"{sport_name} monitoring task has stopped.")

async def main_async_loop():
    loop = asyncio.get_running_loop()

    # Start Firestore listener with dispatcher callback
    firestore_manager.listen_sports_flags(lambda sport_name, active, league_id: dispatcher_callback(sport_name, active, league_id, loop))

    # Start the Realtime Database listener for other tasks
    logging.info("Starting Realtime Database listener for players...")
    realtime_listener.start()

    # Run until the shutdown event is triggered
    while not shutdown_event.is_set():
        if not sport_tasks or all(future.done() for future in sport_tasks.values()):
            logging.info("No sports currently being monitored.")
        await asyncio.sleep(10)

    logging.info("Main loop received shutdown signal, cleaning up...")

    # Gracefully stop Firestore listener
    firestore_manager.stop_listener()

    # Gracefully stop all running sports monitoring tasks
    for sport, future in sport_tasks.items():
        if not future.done():
            logging.info(f"Cancelling monitoring task for {sport}.")
            future.cancel()
            try:
                await asyncio.wrap_future(future)
            except asyncio.CancelledError:
                logging.info(f"{sport} monitoring task cancelled successfully.")

    logging.info("All monitoring tasks cleaned up.")

if __name__ == "__main__":
    signal_handler.register_signal_handlers()

    try:
        asyncio.run(main_async_loop())
    except KeyboardInterrupt:
        logging.info("Program terminated by user (KeyboardInterrupt).")
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
    finally:
        logging.info("Application shutdown complete.")
