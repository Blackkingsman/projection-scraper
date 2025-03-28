import logging
import threading
import time
from managers.firebase_manager import FirebaseManager
from managers.cache_manager import CacheManager

class RealtimeListener:
    def __init__(self, firebase_manager: FirebaseManager, cache_manager: CacheManager, platform_abbr: str):
        self.firebase_manager = firebase_manager
        self.cache_manager = cache_manager
        self.platform_abbr = platform_abbr.lower()
        self.listener_running = True
        self.initial_sync_complete = threading.Event()

    def warm_up_projections_from_firebase(self, ref_path: str):
        """
        Pull all projection-related data (including player metadata) from Firebase
        and warm up the projection cache with full player records.
        """
        try:
            logging.info(f"[warm_up_projections_from_firebase] Warming cache from Firebase ref: {ref_path}")
            projections_snapshot = self.firebase_manager.get_projections(ref_path)

            if not projections_snapshot:
                logging.warning(f"No projections found in Firebase at {ref_path}")
                return

            league = self.firebase_manager._extract_league_from_ref(ref_path)

            count = self.cache_manager.set_projection_bulk(
                projections_by_player=projections_snapshot,
                league=league
            )

            logging.info(f"[warm_up_projections_from_firebase] Cached {count} full player entries from '{ref_path}'.")

            for player_id in projections_snapshot:
                sample_key = f"{self.platform_abbr}:projections:{league}:{player_id}"
                raw = self.cache_manager.get(sample_key)
                logging.info(f"[Sample Cached Entry] {sample_key} = {raw}")
                break

        except Exception as e:
            logging.error(f"[warm_up_projections_from_firebase] Error warming projection cache: {e}")

    def warm_up_players_from_firebase(self):
        """
        Pull all players from Firebase and populate in-memory cache.
        Always runs on start since cache is not persistent.
        """
        try:
            logging.info("Warming in-memory cache with player data from Firebase.")
            start_time = time.time()

            snapshot = self.firebase_manager.player_ref.get()
            logging.info(f"Snapshot type: {type(snapshot)}")

            if not snapshot:
                logging.warning("No player data found in Firebase.")
                self.initial_sync_complete.set()
                return

            if isinstance(snapshot, list):
                players_dict = {
                    (player.get("player_id") or str(idx)): player
                    for idx, player in enumerate(snapshot) if player
                }
                count = self.cache_manager.bulk_set_players(players_dict)
            elif isinstance(snapshot, dict):
                count = self.cache_manager.bulk_set_players(snapshot)
            else:
                logging.error(f"Unsupported data type from Firebase: {type(snapshot)}")
                self.initial_sync_complete.set()
                return

            duration = time.time() - start_time
            logging.info(f"Cached {count} players from Firebase in {duration:.2f} seconds.")

        except Exception as e:
            logging.error(f"[warm_up_players_from_firebase] Error: {e}")
        finally:
            self.initial_sync_complete.set()

    def start(self):
        """
        Start the Firebase Realtime Database listener.
        """
        def update_cache(event):
            if not self.listener_running:
                logging.info("Listener stopped.")
                return

            event_path = event.path
            updated_data = event.data

            if event_path == "/":
                logging.info("Initial sync from Firebase complete.")
                if isinstance(updated_data, dict):
                    self.cache_manager.bulk_set_players(updated_data)
                elif updated_data is None:
                    logging.info("Initial sync received, but no data available.")
                else:
                    logging.warning(f"Unexpected format for root-level data: {type(updated_data)}")
                self.initial_sync_complete.set()
                return

            logging.debug(f"Data changed for event: {event_path}")
            path_parts = event_path.strip('/').split('/')
            player_id = path_parts[0]

            if updated_data is None:
                logging.debug(f"Data at {event_path} was deleted.")
                self.cache_manager.remove_player(player_id)
                return

            self.cache_manager.set_player(player_id, updated_data)

        threading.Thread(
            target=lambda: self.firebase_manager.player_ref.listen(update_cache),
            daemon=True
        ).start()
