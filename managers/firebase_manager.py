import firebase_admin
from firebase_admin import credentials, db
import json
import sys
import logging
from managers.cache_manager import CacheManager

class FirebaseManager:
    def __init__(self, service_account_key: str, database_url: str, cache_manager: CacheManager, platform_abbr: str):
        cred = credentials.Certificate(service_account_key)
        firebase_admin.initialize_app(cred, {
            'databaseURL': database_url
        })

        self.cache_manager = cache_manager
        self.platform_abbr = platform_abbr.lower()

        if self.platform_abbr == "pp":
            self.player_ref = db.reference('players')
        else:
            self.player_ref = db.reference(f'players{self.platform_abbr.upper()}')

    def _get_projection_ref(self, ref_path: str):
        return db.reference(ref_path)

    def get_player(self, player_id):
        return self.player_ref.child(player_id).get()

    def update_player(self, player_id, player_data):
        try:
            self.player_ref.child(player_id).update(player_data)
            logging.info(f"Updated player {player_id} in Firebase.")
            self.cache_manager.set_player(player_id, player_data)
        except Exception as e:
            logging.error(f"Failed to update player {player_id}: {e}")

    def get_players_by_league(self, league):
        try:
            return self.player_ref.order_by_child('league').equal_to(league).get()
        except Exception as e:
            logging.error(f"Failed to retrieve players for league {league}: {e}")
            return {}

    def update_projections(self, players_with_changes, ref_path: str, chunk_size_mb=16):
        if not players_with_changes:
            logging.warning("No players with changes to update.")
            return

        projection_ref = self._get_projection_ref(ref_path)
        max_chunk_size = chunk_size_mb * 1024 * 1024
        chunk = {}
        chunk_size = 0
        successfully_uploaded_players = {}

        logging.info(f"Preparing to update projections for {len(players_with_changes)} players...")

        for player_id, player_data in players_with_changes.items():
            try:
                player_json = json.dumps({player_id: player_data})
                player_size = sys.getsizeof(player_json)
            except Exception as e:
                logging.error(f"[Serialization] Failed for player {player_id}: {e}")
                continue

            if chunk_size + player_size > max_chunk_size:
                try:
                    projection_ref.update(chunk)
                    logging.info(f"[Firebase Upload] Uploaded chunk with {len(chunk)} players.")
                    successfully_uploaded_players.update(chunk)
                except Exception as e:
                    logging.error(f"[Firebase Upload] Chunk upload failed: {e}")
                chunk = {}
                chunk_size = 0

            chunk[player_id] = player_data
            chunk_size += player_size

        if chunk:
            try:
                projection_ref.update(chunk)
                logging.info(f"[Firebase Upload] Uploaded final chunk with {len(chunk)} players.")
                successfully_uploaded_players.update(chunk)
            except Exception as e:
                logging.error(f"[Firebase Upload] Final chunk upload failed: {e}")

        logging.info(f"✅ Total players successfully updated in Firebase: {len(successfully_uploaded_players)}")


    def get_projections(self, ref_path: str):
        try:
            projection_ref = self._get_projection_ref(ref_path)
            return projection_ref.get()
        except Exception as e:
            logging.error(f"Failed to retrieve projections from '{ref_path}': {e}")
            return {}

    def delete_projections(self, projections_to_remove, ref_path: str, chunk_size_mb=16):
        if not projections_to_remove:
            logging.warning("No projections to remove.")
            return

        projection_ref = self._get_projection_ref(ref_path)
        max_chunk_size = chunk_size_mb * 1024 * 1024
        chunk = {}
        chunk_size = 0
        
        for player_id, projection_id in projections_to_remove:
            logging.info(f"WE want to remove: {player_id} : {projection_id}")
            chunk[f"{player_id}/projections/{projection_id}"] = None
            player_json = json.dumps({player_id: {projection_id: None}})
            player_size = sys.getsizeof(player_json)
        
            if chunk_size + player_size > max_chunk_size:
                try:
                    projection_ref.update(chunk)
                    logging.info(f"Deleted {len(chunk)} projections.")
                    for path in chunk.keys():
                        p_id, proj_id = path.split('/projections/')
                        self.cache_manager.remove_projection(p_id, proj_id, ref_path)
                except Exception as e:
                    logging.error(f"Error deleting chunk of projections: {e}")
                chunk = {}
                chunk_size = 0

            chunk_size += player_size

        if chunk:
            try:
                projection_ref.update(chunk)
                logging.info(f"Deleted remaining {len(chunk)} projections.")
                for path in chunk.keys():
                    p_id, proj_id = path.split('/projections/')
                    self.cache_manager.remove_projection(p_id, proj_id, ref_path)
            except Exception as e:
                logging.error(f"Error deleting remaining projections: {e}")

    def _extract_league_from_ref(self, ref_path: str) -> str:
        """
        Extract league abbreviation from a Firebase ref path like 'prizepicksNBA' -> 'NBA'.
        """
        try:
            return ref_path.replace(self.platform_abbr, "").replace("prizepicks", "").upper()
        except Exception:
            return "GENERIC"
