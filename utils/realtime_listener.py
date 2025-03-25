# realtime_listener.py

import logging
import threading
from managers.firebase_manager import FirebaseManager
from managers.redis_manager import RedisManager

class RealtimeListener:
    def __init__(self, firebase_manager:FirebaseManager, redis_manager: RedisManager ):
        self.firebase_manager = firebase_manager
        self.redis_manager = redis_manager
        self.listener_running = True

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
                    for player_id, player_data in updated_data.items():
                        if self.redis_manager.set_player(player_id, player_data):
                            logging.info(f"Set player {player_id} data in Redis (initial sync).")
                        else:
                            logging.error(f"Failed to set player {player_id} data in Redis (initial sync).")
                elif updated_data is None:
                    logging.info("Initial sync received, but no data available.")
                else:
                    logging.warning(f"Unexpected format for root-level data: {type(updated_data)}")
                return

            logging.info(f"Data changed for event: {event_path}")
            path_parts = event_path.strip('/').split('/')
            player_id = path_parts[0]

            if updated_data is None:
                logging.info(f"Data at {event_path} was deleted.")
                self.redis_manager.remove_player(player_id)
                return

            if self.redis_manager.set_player(player_id, updated_data):
                logging.info(f"Set player {player_id} data in Redis.")
            else:
                logging.error(f"Failed to set player {player_id} data in Redis.")


        # Start the listener thread
        threading.Thread(target=lambda: self.firebase_manager.player_ref.listen(update_cache), daemon=True).start()


