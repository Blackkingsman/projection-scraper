import os
import json
import random
import asyncio
import logging
import threading
from enum import Enum

from managers.firebase_manager import FirebaseManager
from managers.firestore_manager import FirestoreManager
from managers.cache_manager import CacheManager
from utils.data_fetcher import DataFetcher
from utils.projection_processor import ProjectionProcessor
from utils.signal_handler import SignalHandler
from utils.realtime_listener import RealtimeListener

# ------------------- Platform + Config -------------------

class Platform(Enum):
    PRIZEPICKS = "PrizePicks"

PLATFORM_CONFIG = {
    Platform.PRIZEPICKS: {
        "abbr": "pp",
        "players_ref": "players",
        "projections": {
            7: {"sport": "NBA", "ref": "prizepicksNBA"},
            9: {"sport": "NFL", "ref": "prizepicksNFL"},
            8: {"sport": "NHL", "ref": "prizepicksNHL"},
            2: {"sport": "MLB", "ref": "prizepicksMLB"},
        }
    }
}

selected_platform = Platform.PRIZEPICKS
config = PLATFORM_CONFIG[selected_platform]

# ------------------- Initialization -------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
shutdown_event = threading.Event()

cache_manager = CacheManager(platform_abbr=config["abbr"])
data_fetcher = DataFetcher(config["abbr"])

firebase_manager = FirebaseManager(
    "./config/serviceAccountKey.json",
    "https://sportbets-1e08a-default-rtdb.firebaseio.com/",
    cache_manager,
    platform_abbr=config["abbr"]
)

firestore_manager = FirestoreManager("./config/serviceAccountKey.json")
projection_processor = ProjectionProcessor(cache_manager, firebase_manager, data_fetcher, config["abbr"])
signal_handler = SignalHandler(shutdown_event)
realtime_listener = RealtimeListener(firebase_manager, cache_manager, config["abbr"])

sport_tasks = {}

# ------------------- Dispatcher Callback -------------------

def dispatcher_callback(sport_name: str, active: bool, league_id: str, loop: asyncio.AbstractEventLoop):
    logging.info(f"Dispatcher received update: {sport_name} (League ID: {league_id}) is now {'active' if active else 'inactive'}.")

    league_id_int = int(league_id)
    projection_config = config["projections"].get(league_id_int)

    if not projection_config:
        logging.warning(f"Unknown league ID {league_id} for platform {selected_platform.value}")
        return

    projection_ref = projection_config["ref"]

    if active:
        if sport_name not in sport_tasks or sport_tasks[sport_name].done():
            logging.info(f"Starting monitoring task for {sport_name}.")
            future = asyncio.run_coroutine_threadsafe(monitor_sport(sport_name, league_id, projection_ref), loop)
            sport_tasks[sport_name] = future
        else:
            logging.info(f"Monitoring task for {sport_name} is already running.")
    else:
        future = sport_tasks.get(sport_name)
        if future and not future.done():
            logging.info(f"Stopping monitoring task for {sport_name}.")
            future.cancel()

# ------------------- Monitoring Coroutine -------------------

async def monitor_sport(sport_name: str, league_id: str, projection_ref: str):
    logging.info(f"🟢 Monitor for {sport_name} started with League ID {league_id}.")
    realtime_listener.warm_up_projections_from_firebase(projection_ref)

    try:
        while not shutdown_event.is_set():
            try:
                projections = await data_fetcher.fetch_projections(league_id, selected_platform.value.lower())
            except Exception as e:
                logging.error(f"[{sport_name}] ❌ Error fetching projections: {e}")
                projections = None

            logging.info(f"Projection length {len(projections)} for sport {sport_name}")

            if projections:
                filtered_projections = projection_processor.filter_relevant_projections(projections, projection_ref)
                if filtered_projections:
                    logging.info(f"[{sport_name}] 📊 Processing {len(filtered_projections)} projections.")
                    remaining_projections = projection_processor.process_projections(filtered_projections, projection_ref)
                    if remaining_projections:
                        logging.info(f"[{sport_name}] 🔍 Fetching additional data for {len(remaining_projections)} players.")
                        await projection_processor.fetch_remaining_players(remaining_projections, projection_ref)
                    else:
                        logging.info(f"[{sport_name}] ✅ No additional projections to fetch.")
                else:
                    logging.info(f"[{sport_name}] 🔄 No relevant changes detected.")
            else:
                logging.warning(f"[{sport_name}] ⚠️ No projections fetched or error occurred.")

            await asyncio.sleep(random.randint(30, 45))

    except asyncio.CancelledError:
        logging.info(f"🛑 Monitoring for {sport_name} cancelled gracefully.")
        if not shutdown_event.is_set():
            realtime_listener.cleanup_projections_by_league(projection_ref)
        raise
    finally:
        logging.info(f"🔚 {sport_name} monitoring task has stopped.")

# ------------------- Main Loop -------------------

async def main_async_loop():
    loop = asyncio.get_running_loop()

    logging.info("Warming Redis cache with initial player data from Firebase...")
    try:
        realtime_listener.warm_up_players_from_firebase()
    except Exception as e:
        logging.error(f"Error during warm up: {e}")

    logging.info("Starting Realtime Database listener for players...")
    realtime_listener.start()

    logging.info("Waiting for Redis cache sync to complete...")
    realtime_listener.initial_sync_complete.wait()
    logging.info("Initial Redis cache loaded.")

    firestore_manager.listen_sports_flags(lambda sport_name, active, league_id: dispatcher_callback(sport_name, active, league_id, loop))

    while not shutdown_event.is_set():
        if not sport_tasks or all(future.done() for future in sport_tasks.values()):
            logging.info("No sports currently being monitored.")
        await asyncio.sleep(10)

    logging.info("Main loop received shutdown signal, cleaning up...")
    firestore_manager.stop_listener()

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
