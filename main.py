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
from managers.league_activity_manager import LeagueActivityManager

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

# Shared dictionary to track sports signaling shutdown
shutdown_signals = {}

# ------------------- Dispatcher Callback -------------------

def dispatcher_callback(sport_name: str, active: bool, league_id: str, loop: asyncio.AbstractEventLoop, league_activity_manager: LeagueActivityManager) -> None:
    """
    Callback function triggered by Firestore updates to start or stop monitoring tasks.

    Args:
        sport_name (str): Name of the sport (e.g., NBA, NFL).
        active (bool): Whether the sport is active or not.
        league_id (str): League ID for the sport.
        loop (asyncio.AbstractEventLoop): The asyncio event loop.
        league_activity_manager (LeagueActivityManager): Manager for league activity.
    """
    logging.info(f"Dispatcher received update: {sport_name} (League ID: {league_id}) is now {'active' if active else 'inactive'}.")

    league_id_int = int(league_id)
    projection_config = config["projections"].get(league_id_int)

    if not projection_config:
        logging.warning(f"Unknown league ID {league_id} for platform {selected_platform.value}")
        return

    projection_ref = projection_config["ref"]
    # Update LeagueActivityManager internal flag status to keep auto-probe in sync
    league_activity_manager.update_league_flag_state(league_id, active)
    
    if active:
        firestore_manager.update_league_flag(sport_name, active=True)
        logging.info(f"[{sport_name}] Firestore flag set to active.")

        if sport_name not in sport_tasks or sport_tasks[sport_name].done():
            logging.info(f"Starting monitoring task for {sport_name}.")
            process_complete_event = threading.Event()
            future = asyncio.run_coroutine_threadsafe(
                monitor_sport(sport_name, league_id, projection_ref, process_complete_event, league_activity_manager), loop
            )
            sport_tasks[sport_name] = future
        else:
            logging.info(f"Monitoring task for {sport_name} is already running.")
    else:
        future = sport_tasks.get(sport_name)
        if future and not future.done():
            logging.info(f"Stopping monitoring task for {sport_name}.")
            future.cancel()
            try:
                # Block until the monitoring task fully stops
                future.result()
                logging.info(f"Monitoring task for {sport_name} stopped successfully.")
            except asyncio.CancelledError:
                logging.info(f"Monitoring task for {sport_name} cancelled successfully.")
            except Exception as e:
                logging.error(f"Unexpected error stopping monitoring task for {sport_name}: {e}")

# ------------------- Monitoring Coroutine -------------------

async def monitor_sport(sport_name: str, league_id: str, projection_ref: str, process_complete_event: threading.Event, league_activity_manager: LeagueActivityManager) -> None:
    """
    Coroutine to monitor projections for a specific sport.

    Args:
        sport_name (str): Name of the sport (e.g., NBA, NFL).
        league_id (str): League ID for the sport.
        projection_ref (str): Firebase reference for projections.
        process_complete_event (threading.Event): Event to signal shutdown.
        league_activity_manager (LeagueActivityManager): Manager for league activity.
    """
    # Create a semaphore to prevent overlapping processing operations
    processing_semaphore = asyncio.Semaphore(1)
    
    logging.info(f"🟢 Monitor for {sport_name} started with League ID {league_id}.")
    realtime_listener.warm_up_projections_from_firebase(projection_ref)

    try:
        # Continue monitoring until global shutdown or this sport signals complete
        while not shutdown_event.is_set() and not process_complete_event.is_set():
            try:
                result = await data_fetcher.fetch_projections(league_id, selected_platform.value.lower())
                status_code = result.get("status_code")
                projections = result.get("projections")

                if not isinstance(projections, list):
                    logging.error(f"[{sport_name}] Invalid projections format: {type(projections)}")
                    projections = []
            except Exception as e:
                logging.error(f"[{sport_name}] ❌ Error fetching projections: {e}")
                status_code, projections = None, None

            if projections:
                async with processing_semaphore:  # Prevent overlapping processing
                    logging.info(f"[{sport_name}] Retrieved {len(projections)} projections.")
                    filtered_projections = await projection_processor.filter_relevant_projections(projections, projection_ref)
                    if isinstance(filtered_projections, dict) and filtered_projections:
                        logging.info(f"[{sport_name}] 📊 Processing {len(filtered_projections)} projections.")
                        try:
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
            elif status_code == 200 and len(projections) == 0:
                async with processing_semaphore:  # Prevent overlapping cleanup
                    if projection_ref:
                        logging.info(f"[{sport_name}] No projections from API. Cleaning up outdated projections.")
                        historical_ref_path = f"{projection_ref}Historicals"

                        cached_players = cache_manager.get_all_player_projections_by_league(projection_processor.firebase_manager._extract_league_from_ref(projection_ref))

                        if cached_players:
                            # Pass the cached players so the cleanup functions know what to remove
                            await projection_processor.store_historical_projections(cached_players, projection_ref, historical_ref_path)
                            await projection_processor.remove_outdated_projections(cached_players, projection_ref)
                            logging.info(f"[{sport_name}] Cleaned up {len(cached_players)} outdated projections.")
                        else:
                            logging.info(f"[{sport_name}] No cached data found. Signaling shutdown.")
                            process_complete_event.set()
                            shutdown_signals[sport_name] = True
                            # Break loop after signaling shutdown
                            break
                        shutdown_signals[sport_name] = True
                        # Break loop after signaling shutdown
                        break
            else:
                logging.info(f"[{sport_name}] Continuing monitoring as projections are still available.")

            await asyncio.sleep(random.randint(30, 45))

    except asyncio.CancelledError:
        logging.info(f"🛑 Monitoring for {sport_name} cancelled gracefully.")
        if not shutdown_event.is_set():
            realtime_listener.cleanup_projections_by_league(projection_ref)
        raise
    finally:
        logging.info(f"🔚 {sport_name} monitoring task has stopped.")

        # Quick restart check - don't hand off to league watcher if we can restart immediately
        logging.info(f"[{sport_name}] Checking if immediate restart is needed...")
        
        try:
            projections_count = await league_activity_manager.get_projections_count_for_league(league_id)
            logging.info(f"[{sport_name}] Final check: {projections_count} projections available")
            
            if projections_count > 0 and not shutdown_event.is_set():
                # Check if there's already a task running for this sport to avoid duplicates
                current_task = sport_tasks.get(sport_name)
                if current_task is None or current_task.done():
                    logging.info(f"[{sport_name}] Projections exist and no active task - starting immediate restart.")
                    # Start new monitoring task directly AND update Firestore for external processes
                    process_complete_event = threading.Event()
                    current_loop = asyncio.get_running_loop()
                    future = asyncio.run_coroutine_threadsafe(
                        monitor_sport(sport_name, league_id, config["projections"][int(league_id)]["ref"], process_complete_event, league_activity_manager), 
                        current_loop
                    )
                    sport_tasks[sport_name] = future
                    
                    # Update Firestore flag for external processes (but we've already started the task)
                    # The dispatcher callback will see the task is already running and skip starting a duplicate
                    firestore_manager.update_league_flag(sport_name, active=True)
                    logging.info(f"[{sport_name}] Immediate restart complete - task started and Firestore updated for external processes.")
                else:
                    logging.info(f"[{sport_name}] Projections exist but task already running - just ensuring Firestore flag is correct.")
                    # Task is running but make sure Firestore reflects this for external processes
                    firestore_manager.update_league_flag(sport_name, active=True)
            else:
                if shutdown_event.is_set():
                    logging.info(f"[{sport_name}] Shutdown in progress - no restart attempted.")
                else:
                    logging.info(f"[{sport_name}] No projections found - leaving inactive for league watcher.")
                
        except Exception as e:
            logging.error(f"[{sport_name}] Error during immediate restart check: {e}")
            logging.info(f"[{sport_name}] Restart failed, league watcher will handle recovery.")

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

    # Initialize LeagueActivityManager and load initial flags
    watched_ids = list(config["projections"].keys())  # List of league IDs for readiness
    league_activity_manager = LeagueActivityManager(config["abbr"], firestore_manager, watched_league_ids=watched_ids)
    # Load initial Firestore flag states and subscribe to updates (sets readiness event)
    league_activity_manager.start_flag_listener()
    
    # Do an initial league probe immediately on startup
    logging.info("Performing initial league probe on startup...")
    try:
        leagues = await league_activity_manager.fetch_leagues()
        logging.info(f"Initial probe: Found {len(leagues)} leagues from API")
        
        # Filter to only our watched leagues (same as auto-probe does)
        watched_leagues = [league for league in leagues if int(league.get("id", -1)) in watched_ids]
        logging.info(f"Initial probe: Filtered to {len(watched_leagues)} watched leagues: {watched_ids}")
        
        for league in watched_leagues:
            league_id = str(league.get("id", 0))
            league_name = league.get("attributes", {}).get("name", "Unknown")
            count = league.get("attributes", {}).get("projections_count", 0)
            active = count > 0
            
            logging.info(f"Initial probe: League {league_name} (ID: {league_id}) has {count} projections")
            
            if active:
                logging.info(f"Initial probe: Activating league {league_name} (ID: {league_id}) with {count} projections...")
                firestore_manager.update_league_flag(league_name, True)  # Use sport name, not league ID
            else:
                logging.info(f"Initial probe: League {league_name} (ID: {league_id}) has no projections - setting inactive")
                firestore_manager.update_league_flag(league_name, False)  # Use sport name, not league ID
    except Exception as e:
        logging.error(f"Error during initial league probe: {e}")
    
    # Start dispatcher listener for orchestration
    firestore_manager.listen_sports_flags(
        lambda sport_name, active, league_id: dispatcher_callback(
            sport_name, active, league_id, loop, league_activity_manager
        )
    )

    # Start auto league probe in a separate thread with dedicated file logger
    auto_logger = logging.getLogger('league_watcher_auto')
    if not auto_logger.handlers:
        fh = logging.FileHandler('league_watcher_auto.log')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        auto_logger.addHandler(fh)
        auto_logger.setLevel(logging.INFO)
    threading.Thread(
        target=lambda: asyncio.run(league_activity_manager.start_auto_league_probe()),
        daemon=True
    ).start()
    auto_logger.info("Started auto league probe thread.")

    stopped_processes = set()

    while not shutdown_event.is_set():
        for sport_name in list(sport_tasks.keys()):
            future = sport_tasks[sport_name]
            if future.done():
                stopped_processes.add(sport_name)

        for sport_name in stopped_processes:
            # Fast recovery check - don't wait 2-3 minutes for league watcher if task crashed
            league_id = firestore_manager.get_league_id(sport_name)
            # Fetch current projections count for this league
            projections_count = await league_activity_manager.get_projections_count_for_league(league_id)
            if projections_count > 0:
                logging.info(f"Task stopped but projections still active for {sport_name} (count={projections_count}), fast recovery via Firestore update.")
                # Fast recovery - update Firestore flag immediately, dispatcher will handle restart
                firestore_manager.update_league_flag(sport_name, active=True)
                logging.info(f"[{sport_name}] Fast recovery: Firestore flag updated to active - dispatcher will restart monitoring.")
                # Clear any previous shutdown signal
                shutdown_signals.pop(sport_name, None)
            else:
                logging.info(f"Task stopped and no projections for {sport_name}. League inactive.")
                # Don't update Firestore here - let league watcher handle deactivation timing
                # Clear any previous shutdown signal
                shutdown_signals.pop(sport_name, None)
         
        stopped_processes.clear()

        await asyncio.sleep(10)

    logging.info("Main loop received shutdown signal, cleaning up...")
    firestore_manager.stop_listener()

    try:
        await asyncio.gather(
            *(asyncio.wrap_future(future) for future in sport_tasks.values() if not future.done()),
            return_exceptions=True
        )
    except Exception as e:
        logging.error(f"Error during task cleanup: {e}")

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
