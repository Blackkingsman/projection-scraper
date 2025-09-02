import firebase_admin
from firebase_admin import credentials, db
import json
import sys
import logging
from typing import List, Dict, Optional, Any
from managers.cache_manager import CacheManager


class FirebaseManager:
    """
    Manages Firebase interactions, including updating and retrieving player and projection data.
    """

    def __init__(self, service_account_key: str, database_url: str, cache_manager: CacheManager, platform_abbr: str):
        """
        Initialize the FirebaseManager.

        Args:
            service_account_key (str): Path to the Firebase service account JSON file.
            database_url (str): Firebase Realtime Database URL.
            cache_manager (CacheManager): Cache manager for handling Redis caching.
            platform_abbr (str): Abbreviation for the platform (e.g., "pp" for PrizePicks).
        """
        cred = credentials.Certificate(service_account_key)
        firebase_admin.initialize_app(cred, {
            'databaseURL': database_url
        })

        self.cache_manager = cache_manager
        self.platform_abbr = platform_abbr.lower()

        # Set player reference based on platform
        if self.platform_abbr == "pp":
            self.player_ref = db.reference('players')
        else:
            self.player_ref = db.reference(f'players{self.platform_abbr.upper()}')

    def _get_projection_ref(self, ref_path: str):
        """
        Get a Firebase reference for projections.

        Args:
            ref_path (str): Firebase reference path.

        Returns:
            db.Reference: Firebase reference object.
        """
        return db.reference(ref_path)

    def get_player(self, player_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve player data from Firebase.

        Args:
            player_id (str): Player ID.

        Returns:
            Optional[Dict[str, Any]]: Player data if found, otherwise None.
        """
        try:
            return self.player_ref.child(player_id).get()
        except Exception as e:
            logging.error(f"[get_player] Failed to retrieve player {player_id}: {e}")
            return None

    def update_player(self, player_id: str, player_data: Dict[str, Any]) -> None:
        """
        Update player data in Firebase and cache.

        Args:
            player_id (str): Player ID.
            player_data (Dict[str, Any]): Player data to update.
        """
        try:
            self.player_ref.child(player_id).update(player_data)
            logging.info(f"[update_player] Updated player {player_id} in Firebase.")
            self.cache_manager.set_player(player_id, player_data)
        except Exception as e:
            logging.error(f"[update_player] Failed to update player {player_id}: {e}")

    def get_players_by_league(self, league: str) -> Dict[str, Any]:
        """
        Retrieve all players for a specific league.

        Args:
            league (str): League abbreviation (e.g., "NBA").

        Returns:
            Dict[str, Any]: Players grouped by league.
        """
        try:
            return self.player_ref.order_by_child('league').equal_to(league).get()
        except Exception as e:
            logging.error(f"[get_players_by_league] Failed to retrieve players for league {league}: {e}")
            return {}

    def update_projections(self, players_with_changes: Dict[str, Dict[str, Any]], ref_path: str, chunk_size_mb: int = 16) -> None:
        """
        Update player projections in Firebase in chunks, merging with existing projections.

        Args:
            players_with_changes (Dict[str, Dict[str, Any]]): Players with updated projections.
            ref_path (str): Firebase reference path for projections.
            chunk_size_mb (int): Maximum chunk size in megabytes.
        """
        if not players_with_changes:
            logging.warning("[update_projections] No players with changes to update.")
            return

        projection_ref = self._get_projection_ref(ref_path)
        max_chunk_size = chunk_size_mb * 1024 * 1024
        chunk = {}
        chunk_size = 0
        successfully_uploaded_players = {}

        logging.info(f"[update_projections] Preparing to update projections for {len(players_with_changes)} players.")
        league = self._extract_league_from_ref(ref_path)

        for player_id, player_data in players_with_changes.items():
            # CRITICAL FIX: Merge new projections with existing cache BEFORE sending to Firebase
            merged_data = self._prepare_merged_firebase_data(player_id, player_data, league)
            
            try:
                player_json = json.dumps({player_id: merged_data})
                player_size = sys.getsizeof(player_json)
            except Exception as e:
                logging.error(f"[update_projections] Serialization failed for player {player_id}: {e}")
                continue

            if chunk_size + player_size > max_chunk_size:
                try:
                    projection_ref.update(chunk)
                    logging.info(f"[update_projections] Uploaded chunk with {len(chunk)} players.")
                    successfully_uploaded_players.update(chunk)

                    # Update cache with the merged data
                    for pid, pdata in chunk.items():
                        self.cache_manager.set_projection(pid, pdata, league)

                except Exception as e:
                    logging.error(f"[update_projections] Chunk upload failed: {e}")

                chunk = {}
                chunk_size = 0

            chunk[player_id] = merged_data
            chunk_size += player_size

        if chunk:
            try:
                projection_ref.update(chunk)
                logging.info(f"[update_projections] Uploaded final chunk with {len(chunk)} players.")
                successfully_uploaded_players.update(chunk)

                # Update cache with the merged data
                for pid, pdata in chunk.items():
                    self.cache_manager.set_projection(pid, pdata, league)

            except Exception as e:
                logging.error(f"[update_projections] Final chunk upload failed: {e}")

        logging.info(f"[update_projections] Total players successfully updated in Firebase: {len(successfully_uploaded_players)}")

    def _prepare_merged_firebase_data(self, player_id: str, new_data: Dict[str, Any], league: str) -> Dict[str, Any]:
        """
        Prepare merged data for Firebase by combining existing cache projections with new data.
        
        Args:
            player_id: Player ID
            new_data: New projection data containing only new/changed projections
            league: League abbreviation
            
        Returns:
            Complete player data with all projections merged
        """
        try:
            # Get existing cache data
            existing_data = self.cache_manager.get_projection_by_league(player_id, league)
            
            if existing_data:
                # Merge projections - new data takes precedence for conflicts
                existing_projections = existing_data.get("projections", {})
                new_projections = new_data.get("projections", {})
                
                # Merge the projection dictionaries
                merged_projections = {**existing_projections, **new_projections}
                
                # Create merged data with updated projections and metadata
                merged_data = {**existing_data, **new_data}
                merged_data["projections"] = merged_projections
                
                # Debug A.J. Brown merging
                if player_id == "206304":
                    logging.warning(f"[AJ_BROWN_DEBUG] Firebase merge - existing: {len(existing_projections)}, new: {len(new_projections)}, merged: {len(merged_projections)}")
                
                return merged_data
            else:
                # No existing data, return new data as-is
                return new_data
                
        except Exception as e:
            logging.error(f"[_prepare_merged_firebase_data] Error preparing merged data for player {player_id}: {e}")
            # Fallback to new data only
            return new_data

    def _merge_cache_projections(self, player_id: str, new_data: Dict[str, Any], league: str) -> None:
        """
        DEPRECATED: This method is no longer used. 
        Merge logic is now handled in _prepare_merged_firebase_data before Firebase upload.
        """
        pass

    def set_projections(self, ref_path: str, data: Optional[Dict[str, Any]]) -> None:
        """
        Set or delete projections at the given path.

        Args:
            ref_path (str): Firebase reference path for projections.
            data (Optional[Dict[str, Any]]): Data to set. If None, deletes the node.
        """
        try:
            ref = self._get_projection_ref(ref_path)
            if data is None:
                ref.delete()
                logging.info(f"[set_projections] Deleted projections at '{ref_path}'")
            else:
                ref.set(data)
                logging.info(f"[set_projections] Set projections at '{ref_path}'")
        except Exception as e:
            logging.error(f"[set_projections] Failed to set projections at {ref_path}: {e}")

    def get_projections(self, ref_path: str):
        try:
            projection_ref = self._get_projection_ref(ref_path)
            return projection_ref.get()
        except Exception as e:
            logging.error(f"Failed to retrieve projections from '{ref_path}': {e}")
            return {}

    def delete_projections(self, projections_to_remove: List[tuple], ref_path: str, chunk_size_mb: int = 16) -> None:
        """
        Delete specific projections from Firebase with atomic operations.

        Args:
            projections_to_remove (List[tuple]): List of (player_id, projection_id) tuples to remove.
            ref_path (str): Firebase reference path for projections.
            chunk_size_mb (int): Maximum chunk size in megabytes.
        """
        if not projections_to_remove:
            logging.warning("[delete_projections] No projections to remove.")
            return

        projection_ref = self._get_projection_ref(ref_path)
        league_abbr = self._extract_league_from_ref(ref_path)
        max_chunk_size = chunk_size_mb * 1024 * 1024
        chunk = {}
        chunk_size = 0
        successful_deletions = []

        for player_id, projection_id in projections_to_remove:
            logging.info(f"[delete_projections] Queued for deletion -> Player: {player_id}, Projection: {projection_id}")
            chunk[f"{player_id}/projections/{projection_id}"] = None
            player_json = json.dumps({player_id: {projection_id: None}})
            player_size = sys.getsizeof(player_json)

            if chunk_size + player_size > max_chunk_size:
                try:
                    projection_ref.update(chunk)
                    logging.info(f"[delete_projections] Deleted {len(chunk)} projections from Firebase.")
                    
                    # Only update cache after successful Firebase deletion
                    for path in chunk.keys():
                        p_id, proj_id = path.split('/projections/')
                        self.cache_manager.remove_projection(p_id, proj_id, league_abbr)
                        successful_deletions.append((p_id, proj_id))
                        
                except Exception as e:
                    logging.error(f"[delete_projections] Error deleting chunk of projections: {e}")
                    # Don't update cache if Firebase operation failed
                    
                chunk = {}
                chunk_size = 0

            chunk_size += player_size

        # Handle final chunk
        if chunk:
            try:
                projection_ref.update(chunk)
                logging.info(f"[delete_projections] Deleted final {len(chunk)} projections from Firebase.")
                
                # Only update cache after successful Firebase deletion
                for path in chunk.keys():
                    p_id, proj_id = path.split('/projections/')
                    self.cache_manager.remove_projection(p_id, proj_id, league_abbr)
                    successful_deletions.append((p_id, proj_id))
                    
            except Exception as e:
                logging.error(f"[delete_projections] Error deleting remaining projections: {e}")

        logging.info(f"[delete_projections] Successfully deleted {len(successful_deletions)} out of {len(projections_to_remove)} projections.")

    def delete_entire_player_nodes(self, player_ids: List[str], ref_path: str, chunk_size_mb: int = 16) -> None:
        """
        Delete entire player nodes from Firebase with atomic operations.

        Args:
            player_ids (List[str]): List of player IDs to delete.
            ref_path (str): Firebase reference path for projections.
            chunk_size_mb (int): Maximum chunk size in megabytes.
        """
        if not player_ids:
            logging.info("[delete_entire_player_nodes] No player nodes to delete.")
            return

        projection_ref = self._get_projection_ref(ref_path)
        max_chunk_size = chunk_size_mb * 1024 * 1024
        chunk = {}
        chunk_size = 0
        successful_deletions = []

        league = self._extract_league_from_ref(ref_path)

        try:
            for player_id in player_ids:
                path = f"{player_id}"
                chunk[path] = None

                # Estimate size
                player_json = json.dumps({path: None})
                player_size = sys.getsizeof(player_json)

                if chunk_size + player_size > max_chunk_size:
                    try:
                        projection_ref.update(chunk)
                        logging.info(f"[delete_entire_player_nodes] Deleted {len(chunk)} player projection nodes.")
                        
                        # Only update cache after successful Firebase deletion
                        for pid in chunk.keys():
                            self.cache_manager.remove_player_projections(pid, league)
                            successful_deletions.append(pid)
                            
                    except Exception as e:
                        logging.error(f"[delete_entire_player_nodes] Error deleting chunk of player nodes: {e}")
                        
                    chunk = {}
                    chunk_size = 0

                chunk_size += player_size

            # Final chunk
            if chunk:
                try:
                    projection_ref.update(chunk)
                    logging.info(f"[delete_entire_player_nodes] Deleted final {len(chunk)} player projection nodes.")
                    
                    # Only update cache after successful Firebase deletion
                    for pid in chunk.keys():
                        self.cache_manager.remove_player_projections(pid, league)
                        successful_deletions.append(pid)
                        
                except Exception as e:
                    logging.error(f"[delete_entire_player_nodes] Error deleting remaining player nodes: {e}")

            logging.info(f"[delete_entire_player_nodes] Successfully deleted {len(successful_deletions)} out of {len(player_ids)} player nodes.")

        except Exception as e:
            logging.error(f"[delete_entire_player_nodes] Error deleting player projection nodes: {e}")

    def _extract_league_from_ref(self, ref_path: str) -> str:
        """
        Extract league abbreviation from a Firebase ref path (e.g., 'prizepicksNBA' -> 'NBA').

        Args:
            ref_path (str): Firebase reference path.

        Returns:
            str: League abbreviation.
        """
        try:
            return ref_path.replace(self.platform_abbr, "").replace("prizepicks", "").upper()
        except Exception:
            return "GENERIC"
