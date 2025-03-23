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
        logging.info("Starting Realtime Database listener for NFL players...")

        def update_cache(event):
            if not self.listener_running:
                logging.info("Listener stopped.")
                return

            event_path = event.path
            updated_data = event.data
            logging.info(f"Data changed for event: {event_path}")

            path_parts = event_path.strip('/').split('/')
            player_id = path_parts[0]

            # Handle data deletions
            if updated_data is None:
                logging.info(f"Data at {event_path} was deleted.")
                self.redis_manager.remove_player(player_id)
                return

            # Handle data updates
            if self.redis_manager.set_player(player_id, updated_data):
                logging.info(f"Set player {player_id} data in Redis.")
            else:
                logging.error(f"Failed to set player {player_id} data in Redis.")


        # Start the listener thread
        threading.Thread(target=lambda: self.firebase_manager.player_ref.listen(update_cache), daemon=True).start()


