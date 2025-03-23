import firebase_admin
from firebase_admin import credentials, db
import json
import sys
import logging
from managers.redis_manager import RedisManager

class FirebaseManager:
    def __init__(self, service_account_key:str, database_url:str, redis_manager:RedisManager):
        # Initialize Firebase with the provided credentials and database URL
        cred = credentials.Certificate(service_account_key)
        firebase_admin.initialize_app(cred, {
            'databaseURL': database_url
        })
       
        self.redis_manager = redis_manager
        self.player_ref = db.reference('players')
        self.nfl_projection_ref = db.reference('prizepicksNFL')

    def get_player(self, player_id):
        """
        Get player data from Firebase.
        """
        return self.player_ref.child(player_id).get()

    def update_player(self, player_id, player_data):
        """
        Update player data in Firebase and cache.
        """
        try:
            self.player_ref.child(player_id).update(player_data)
            logging.info(f"Updated player {player_id} in Firebase.")
            # Update the local player cache after successful upload
           
            self.redis_manager.set_player(player_id, player_data)
        except Exception as e:
            logging.error(f"Failed to update player {player_id}: {e}")

    def get_players_by_league(self, league):
        """
        Get all players by league from Firebase.
        """
        try:
            return self.player_ref.order_by_child('league').equal_to(league).get()
        except Exception as e:
            logging.error(f"Failed to retrieve players for league {league}: {e}")
            return {}

    def update_projections(self, players_with_changes, chunk_size_mb=16):
        """
        Update player projections in Firebase, ensuring chunks don't exceed 16MB, and merge projections using cache.
        """
        if not players_with_changes:
            logging.warning("No players with changes to update.")
            return

        max_chunk_size = chunk_size_mb * 1024 * 1024  # 16 MB
        chunk = {}
        chunk_size = 0
        successfully_uploaded_players = {}

        for player_id, player_data in players_with_changes.items():
            # Fetch the player's existing data from the local cache instead of Firebase
            cached_player_data = self.redis_manager.get_projection(player_id)
            existing_projections = cached_player_data.get("projections", {})

            # Merge new projections with existing ones
            new_projections = player_data.get("projections", {})
            existing_projections.update(new_projections)
            player_data["projections"] = existing_projections
            logging.info(f"Merged projections for player {player_id} from cache.")

            player_json = json.dumps({player_id: player_data})
            player_size = sys.getsizeof(player_json)

            if chunk_size + player_size > max_chunk_size:
                # Upload the chunk to Firebase
                try:
                    self.nfl_projection_ref.update(chunk)
                    logging.info(f"Uploaded chunk with {len(chunk)} players to Firebase.")
                    # Update cache after successful upload
                    successfully_uploaded_players.update(chunk)
                except Exception as e:
                    logging.error(f"Error uploading chunk to Firebase: {e}")
                chunk = {}
                chunk_size = 0

            chunk[player_id] = player_data
            chunk_size += player_size

        if chunk:
            try:
                self.nfl_projection_ref.update(chunk)
                logging.info(f"Uploaded final chunk with {len(chunk)} players to Firebase.")
                # Update cache after final chunk upload
                successfully_uploaded_players.update(chunk)
            except Exception as e:
                logging.error(f"Error uploading final chunk to Firebase: {e}")

        # After all uploads are successful, update the PrizePicks NFL cache
        for player_id, player_data in successfully_uploaded_players.items():
          self.redis_manager.update_projection_cache(player_id, player_data["projections"])
           
    def get_projections(self):
        """
        Get all NFL projections from the `prizepicksNFL` reference in Firebase.
        """
        try:
            return self.nfl_projection_ref.get()
        except Exception as e:
            logging.error(f"Failed to retrieve NFL projections: {e}")
            return {}

    def delete_projections(self, projections_to_remove, chunk_size_mb=16):
        """
        Delete outdated projections from Firebase in chunks, ensuring chunks don't exceed 16MB,
        and update cache.
        """
        if not projections_to_remove:
            logging.warning("No projections to remove.")
            return

        max_chunk_size = chunk_size_mb * 1024 * 1024  # 16 MB
        chunk = {}
        chunk_size = 0

        for player_id, projection_id in projections_to_remove:
            # Add projection to the chunk for deletion
            chunk[f"{player_id}/projections/{projection_id}"] = None
            player_json = json.dumps({player_id: {projection_id: None}})
            player_size = sys.getsizeof(player_json)

            # If the chunk size exceeds the limit, send the current chunk to Firebase
            if chunk_size + player_size > max_chunk_size:
                try:
                    self.nfl_projection_ref.update(chunk)
                    logging.info(f"Deleted {len(chunk)} projections.")
                    # Update cache after deletion
                    for path in chunk.keys():
                        p_id, proj_id = path.split('/projections/')
                        self.redis_manager.remove_projection(p_id, proj_id)
                except Exception as e:
                    logging.error(f"Error deleting chunk of projections: {e}")
                chunk = {}
                chunk_size = 0

            chunk_size += player_size

        # Delete any remaining projections in the final chunk
        if chunk:
            try:
                self.nfl_projection_ref.update(chunk)
                logging.info(f"Deleted remaining {len(chunk)} projections.")
                # Update cache after final deletion
                for path in chunk.keys():
                    p_id, proj_id = path.split('/projections/')
                    self.redis_manager.remove_projection(p_id, proj_id)
            except Exception as e:
                logging.error(f"Error deleting remaining projections: {e}")
