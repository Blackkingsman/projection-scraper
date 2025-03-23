import logging
import time
class CacheManager:
    def __init__(self):
        # Cache for players and projections
        self.local_player_cache = {}  # Cache for NFL players
        self.prizepicks_nfl_cache = {}  # Cache for NFL projections
    
    # ----------------------
    # Player Cache Functions
    # ----------------------

    def update_cache(self, player_id, player_data):
        """
        Update the local player cache with player data if the data has changed.
        """
        existing_player_data = self.local_player_cache.get(player_id)

        if not existing_player_data or existing_player_data != player_data:
            # Update the cache only if data is new or changed
            self.local_player_cache[player_id] = player_data
            logging.info(f"Updated local player cache for player {player_id}.")
        else:
            logging.info(f"Player {player_id} data in local player cache is up-to-date; no need to update.")

    def load_cache_from_firebase(self, firebase_manager):
        """
        Load the NFL player cache from Firebase during startup.
        """
        try:
            logging.info("Loading NFL player cache from Firebase on startup...")
            all_players_data = firebase_manager.get_players_by_league("NFL")
            if isinstance(all_players_data, dict):
                self.local_player_cache.update(all_players_data)
                logging.info(f"Player cache initialized with {len(self.local_player_cache)} NFL players.")
            else:
                logging.warning("No valid NFL player data found in Firebase.")
        except Exception as e:
            logging.error(f"Error loading player cache from Firebase: {e}")

    def get_player_from_cache(self, player_id):
        """
        Get a player's information from the local player cache.
        """
        print(self.local_player_cache.get(player_id))
        return self.local_player_cache.get(player_id)

    def remove_player_from_cache(self, player_id):
        """
        Remove a player's data from the local player cache.
        """
        if player_id in self.local_player_cache:
            del self.local_player_cache[player_id]
            logging.info(f"Removed player {player_id} from local player cache.")

    # -------------------------
    # Projection Cache Functions
    # -------------------------
    def get_prizepicks_cachelen (self):
        total_projections = 0
        for playerID in self.prizepicks_nfl_cache:
          total_projections +=  len(self.prizepicks_nfl_cache[playerID]["projections"])
        
        
        return total_projections
    def update_prizepicks_cache(self, player_id, projection_data):
        """
        Update the PrizePicks NFL cache with the new or updated projection data.
        """
        if player_id not in self.prizepicks_nfl_cache:
            self.prizepicks_nfl_cache[player_id] = {"projections": {}}

        # Add or update the player's projections in the PrizePicks cache
        for proj_id, proj_details in projection_data.items():
            self.prizepicks_nfl_cache[player_id]["projections"][proj_id] = proj_details

        logging.info(f"Updated PrizePicks NFL cache for player {player_id} with projections.")

    def load_prizepicks_nfl_cache(self, firebase_manager):
        """
        Load PrizePicks NFL projection cache from Firebase during startup.
        """
        try:
            logging.info("Loading PrizePicks NFL cache from Firebase on startup...")
            all_projections_data = firebase_manager.get_projections()
            if isinstance(all_projections_data, dict):
                self.prizepicks_nfl_cache.update(all_projections_data)
                logging.info(f"PrizePicks NFL cache initialized with {len(self.prizepicks_nfl_cache)} players.")
            else:
                logging.warning("No valid PrizePicks NFL data found in Firebase.")
        except Exception as e:
            logging.error(f"Error loading PrizePicks NFL cache from Firebase: {e}")

    def get_projection_from_cache(self, player_id):
        """
        Get a player's projection from the PrizePicks NFL cache.
        """
        return self.prizepicks_nfl_cache.get(player_id)

    def remove_projection_from_cache(self, player_id, projection_id):
        """
        Remove a player's specific projection from the PrizePicks NFL cache.
        """
        if player_id in self.prizepicks_nfl_cache:
            projections = self.prizepicks_nfl_cache[player_id].get("projections", {})
            if projection_id in projections:
                del projections[projection_id]
                logging.info(f"Removed projection {projection_id} for player {player_id} from cache.")
            if not projections:
                self.prizepicks_nfl_cache.pop(player_id, None)
                logging.info(f"Removed player {player_id} from prizepicks_nfl_cache after all projections deleted.")
