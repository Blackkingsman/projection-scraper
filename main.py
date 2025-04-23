import os
import json
import random
import asyncio
import logging
import threading
import argparse  # Import argparse for command-line argument parsing
from enum import Enum

from managers.firebase_manager import FirebaseManager  # Manages Firebase interactions
from managers.firestore_manager import FirestoreManager  # Listens to Firestore updates
from managers.cache_manager import CacheManager  # Handles Redis caching
from utils.data_fetcher import DataFetcher  # Fetches data from external APIs
from utils.projection_processor import ProjectionProcessor  # Processes projections
from utils.signal_handler import SignalHandler  # Handles OS signals for graceful shutdown
from utils.realtime_listener import RealtimeListener  # Listens to Firebase Realtime Database

# ------------------- Command-Line Arguments -------------------

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run the projection scraper.")
parser.add_argument(
    "-d", "--debug", action="store_true", help="Enable debug mode for detailed logging."
)
args = parser.parse_args()

# ------------------- Debug Mode -------------------

# Enable or disable debug mode based on the flag
DEBUG_MODE = args.debug

# Configure logging level based on debug mode
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Print instructions for enabling/disabling debug mode
print("\n===============================================")
print("Projection Scraper Started")
print("To enable debug mode, run: python main.py -d")
print("To disable debug mode, restart without the -d flag.")
print("To stop the application, press Ctrl+C or kill the terminal.")
print("===============================================\n\n\n")

# Indicate whether debug mode is ON or OFF
if DEBUG_MODE:
    print("********** DEBUG MODE: ON **********\n\n\n")
else:
    print("********** DEBUG MODE: OFF **********\n\n\n")

# ------------------- Platform + Config -------------------

class Platform(Enum):
    PRIZEPICKS = "PrizePicks"

# Configuration for different platforms
PLATFORM_CONFIG = {
    Platform.PRIZEPICKS: {
        "abbr": "pp",
        "players_ref": "players",
        "projections": {
            7: {"sport": "NBA", "ref": "prizepicksNBA"},
            9: {"sport": "NFL", "ref": "prizepicksNFL"},
            8: {"sport": "NHL", "ref": "prizepicksNHL"},
            2: {"sport": "MLB", "ref": "prizepicksMLB"},
        },
    }
}

selected_platform = Platform.PRIZEPICKS
config = PLATFORM_CONFIG[selected_platform]

# ------------------- Initialization -------------------

# Initialize shutdown event for graceful termination
shutdown_event = threading.Event()

# Initialize CacheManager for Redis caching
cache_manager = CacheManager(platform_abbr=config["abbr"])

# Initialize DataFetcher for fetching projections and player data
data_fetcher = DataFetcher(config["abbr"])

# Initialize FirebaseManager for Firebase interactions
firebase_manager = FirebaseManager(
    "./config/serviceAccountKey.json",
    "https://sportbets-1e08a-default-rtdb.firebaseio.com/",
    cache_manager,
    platform_abbr=config["abbr"],
)

# Initialize FirestoreManager for listening to Firestore updates
firestore_manager = FirestoreManager("./config/serviceAccountKey.json")

# Initialize ProjectionProcessor for processing projections
projection_processor = ProjectionProcessor(
    cache_manager, firebase_manager, data_fetcher, config["abbr"]
)

# Initialize SignalHandler for handling OS signals
signal_handler = SignalHandler(shutdown_event)

# Initialize RealtimeListener for Firebase Realtime Database
realtime_listener = RealtimeListener(firebase_manager, cache_manager, config["abbr"])

# Dictionary to track active monitoring tasks
sport_tasks = {}

# ------------------- Dispatcher Callback -------------------

def dispatcher_callback(sport_name: str, active: bool, league_id: str, loop: asyncio.AbstractEventLoop) -> None:
    """
    Callback function triggered by Firestore updates to start or stop monitoring tasks.

    Args:
        sport_name (str): Name of the sport (e.g., NBA, NFL).
        active (bool): Whether the sport is active or not.
        league_id (str): League ID for the sport.
        loop (asyncio.AbstractEventLoop): The asyncio event loop.
    """
    logging.info(f"Dispatcher received update: {sport_name} (League ID: {league_id}) is now {'active' if active else 'inactive'}.")

    league_id_int = int(league_id)
    projection_config = config["projections"].get(league_id_int)

    if not projection_config:
        logging.warning(f"Unknown league ID {league_id} for platform {selected_platform.value}")
        return

    projection_ref = projection_config["ref"]

    if active:
        # Start monitoring task if not already running
        if sport_name not in sport_tasks or sport_tasks[sport_name].done():
            logging.info(f"Starting monitoring task for {sport_name}.")
            future = asyncio.run_coroutine_threadsafe(monitor_sport(sport_name, league_id, projection_ref), loop)
            sport_tasks[sport_name] = future
        else:
            logging.info(f"Monitoring task for {sport_name} is already running.")
    else:
        # Stop monitoring task if active
        future = sport_tasks.get(sport_name)
        if future and not future.done():
            logging.info(f"Stopping monitoring task for {sport_name}.")
            future.cancel()

# ------------------- Monitoring Coroutine -------------------

async def monitor_sport(sport_name: str, league_id: str, projection_ref: str) -> None:
    """
    Coroutine to monitor projections for a specific sport.

    Args:
        sport_name (str): Name of the sport (e.g., NBA, NFL).
        league_id (str): League ID for the sport.
        projection_ref (str): Firebase reference for projections.
    """
    logging.info(f"🟢 Monitor for {sport_name} started with League ID {league_id}.")
    realtime_listener.warm_up_projections_from_firebase(projection_ref)

    try:
        while not shutdown_event.is_set():
            try:
                # Fetch projections from the external API
                projections = await data_fetcher.fetch_projections(league_id, selected_platform.value.lower())
            except Exception as e:
                logging.error(f"[{sport_name}] ❌ Error fetching projections: {e}")
                projections = None

            if projections:
                logging.info(f"[{sport_name}] Retrieved {len(projections)} projections.")
                # Filter relevant projections
                filtered_projections = projection_processor.filter_relevant_projections(projections, projection_ref)
                if filtered_projections:
                    logging.info(f"[{sport_name}] 📊 Processing {len(filtered_projections)} projections.")
                    try:
                        # Process projections and fetch remaining data if needed
                        remaining_projections = await projection_processor.process_projections(filtered_projections, projection_ref)
                    except Exception as e:
                        logging.error(f"[{sport_name}] ❌ Error in process_projections: {e}")
                        remaining_projections = None

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

async def main_async_loop() -> None:
    """
    Main asyncio loop to manage monitoring tasks and listen for Firestore updates.
    """
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

    # Listen for Firestore updates and dispatch tasks
    firestore_manager.listen_sports_flags(
        lambda sport_name, active, league_id: dispatcher_callback(
            sport_name, active, league_id, loop
        )
    )

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
    # Register signal handlers for graceful shutdown
    signal_handler.register_signal_handlers()
    try:
        asyncio.run(main_async_loop())
    except KeyboardInterrupt:
        logging.info("Program terminated by user (KeyboardInterrupt).")
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
    finally:
        logging.info("Application shutdown complete.")
