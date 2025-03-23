import logging
import time
from managers.firebase_manager import FirebaseManager
from managers.redis_manager import RedisManager
from utils.data_fetcher import DataFetcher
class ProjectionProcessor:
    def __init__(self, redis_manager:RedisManager, firebase_manager:FirebaseManager, data_fetcher:DataFetcher):
        """
        Initialize with the required managers and data fetcher.
        """
        self.redis_manager = redis_manager
        self.firebase_manager = firebase_manager
        self.data_fetcher = data_fetcher

    def process_projections(self, projections):
        """
        Process player projections using the local cache and store any remaining projections 
        that require additional player data from the player API endpoint.

        This function is primarily used to complete the projection data by merging the player statistics 
        pulled from PrizePicks with the essential player details (such as player name, position, team, etc.) 
        that are already stored in the cache. 

        For example, the projections data contains only the player ID, but lacks other important 
        information like the player's name and position. These missing details are fetched from the cache 
        to enrich the projection data, making it ready for use within the application.
        """
        # Dictionary to store players with any projections that need to be updated
        players_with_changes = {}

        # Dictionary to store projections for players that need to fetch player data (not found in cache)
        remaining_projections = {}

        logging.info(f"Starting to process {len(projections)} projections...")

        # Iterate through each player and their projections
        for player_id, proj_data in projections.items():
            
            # Try fetching the player info from the local cache only (no external API call yet)
            player_info = self.redis_manager.get_player(player_id)
            # Dictionary to store projection details after filtering unnecessary fields
            filtered_proj_data = {}

            # Loop through each projection for the player and filter out unnecessary details
            for proj_id, details in proj_data.items():
                # Retain only the necessary details for projections: line score, start time, stat type, and status
                filtered_details = {
                    "line_score": details.get("line_score"),
                    "start_time": details.get("start_time"),
                    "stat_type": details.get("stat_type"),
                    "status": details.get("status")
                }
                filtered_proj_data[proj_id] = filtered_details

            # Case: If player is not found in the cache, mark projections as "remaining" and need to fetch player data
            if not player_info:
                logging.info(f"Player {player_id} not found in cache, adding to remaining projections.")

                # If player doesn't exist in `remaining_projections`, initialize an empty dictionary
                if player_id not in remaining_projections:
                    remaining_projections[player_id] = {}

                # Add the filtered projections to the remaining projections list for this player
                remaining_projections[player_id].update(filtered_proj_data)
                
                # Skip the rest of the loop for this player as we don't have the full player info in cache yet
                continue

            # Case: If player exists in the cache, clean up unnecessary fields
            irrelevant_fields = [
                "created_at", "timestamp", "version",  "league_id", "market",
                "oddsjam_id", "team_name", "updated_at", "prizepicks_updated_at"
            ]
            # Remove the irrelevant fields from the player information as we don't need them for projection processing
            for field in irrelevant_fields:
                player_info.pop(field, None)

            # Process the filtered projections for the player
            for proj_id, details in filtered_proj_data.items():
                # Ensure the "projections" key exists in the player data
                if "projections" not in player_info:
                    player_info["projections"] = {}

                # If the projection doesn't exist or it has changed, update it
                if proj_id not in player_info["projections"] or player_info["projections"][proj_id] != details:
                    player_info["projections"][proj_id] = details  # Update the projection with new data
                    logging.info(f"Player {player_id} - Projection {proj_id} flagged for update.")  # Log the update

                    # Add the updated player info to `players_with_changes` to upload to Firebase later
                    if player_id not in players_with_changes:
                        # Store essential player details in the `players_with_changes` dictionary
                        players_with_changes[player_id] = {
                            "name": player_info.get("name"),
                            "position": player_info.get("position"),
                            "team": player_info.get("team"),
                            "league": player_info.get("league"),
                            "projections": {},
                            "jersey_number": player_info.get("jersey_number") if player_info.get("jersey_number") is not None else None,
                            "image_url": player_info.get('image_url') if player_info.get('image_url') is not None else None
                        }

                    # Add the updated projection for this player
                    players_with_changes[player_id]["projections"][proj_id] = details

   
        # If there are any players with changes, upload them to Firebase
        if players_with_changes:
            logging.info(f"Uploading {len(players_with_changes)} players with changes to Firebase.")
            self.firebase_manager.update_projections(players_with_changes)

        # Return any remaining projections that couldn't be processed because the player data is missing
        return remaining_projections

    async def fetch_remaining_players(self, remaining_projections):
        """
        Fetch player data from PrizePicks API for remaining players and process it.
        """
        players_with_changes = {}

        for player_id, proj_data in remaining_projections.items():
            try:
                player_data = await self.data_fetcher.fetch_player_data(player_id)
                if player_data:
                    # Assign projections to player
                    for proj_id, details in proj_data.items():
                        if "projections" not in player_data:
                            player_data["projections"] = {}
                        player_data["projections"][proj_id] = details
                    players_with_changes[player_id] = player_data

                    # Save player information to Firebase
                    self.firebase_manager.update_player(player_id, {
                        "name": player_data.get("name"),
                        "position": player_data.get("position"),
                        "team": player_data.get("team"),
                        "league": player_data.get("league")
                    })
                else:
                    logging.info(f"Player data for {player_id} could not be fetched.")

            except Exception as e:
                logging.error(f"Error fetching player {player_id}: {e}")

        if players_with_changes:
            logging.info(f"Uploading {len(players_with_changes)} remaining players with changes to Firebase.")
            self.firebase_manager.update_projections(players_with_changes)

        remaining_projections = {}  # Clear remaining projections after processing

    def remove_outdated_projections(self, updated_projection_ids):
        """
        Remove outdated projections that are no longer present in the latest API response.
        Delegates the deletion to FirebaseManager.
        """
        projections_to_remove = []

        for player_id, player_info in self.redis_manager.get():
            if "projections" in player_info:
                cached_projections = player_info["projections"]

                for projection_id in list(cached_projections.keys()):
                    if player_id not in updated_projection_ids or projection_id not in updated_projection_ids[player_id]:
                        projections_to_remove.append((player_id, projection_id))
                        logging.info(f"Marking projection {projection_id} for player {player_id} for removal.")

        if projections_to_remove:
            logging.info(f"Removing {len(projections_to_remove)} outdated projections.")
            self.firebase_manager.delete_projections(projections_to_remove)

    def filter_relevant_projections(self, projections):
        """
        Compare projections with cached data and return:
        1. New or updated projections.
        2. Remove outdated projections from Firebase that no longer exist in the API response.
        """

        # Dictionary to store the filtered projections that need to be updated or added
        filtered_projections = {}

        # Dictionary to keep track of the projections that are updated for each player
        updated_projection_ids = {}

        # Sets to track players with specific changes
        new_player_set = set()  # Set of players who are new (not in cache)
        existing_player_with_newProjection_set = set()  # Set of players with a new stat added
        existing_player_with_changedProjection_set = set()  # Set of players with a modified stat

        logging.info(f"Starting filtering of {len(projections)} projections for relevant changes...")

        # Iterate through the projections received from the API
        for proj in projections:
            # Extract player and projection details
            player_id = proj['relationships']['new_player']['data']['id']
            projection_id = proj['id']

            # Extract the projection data: line score, start time, stat type, and status
            line_score = proj['attributes']['line_score']
            start_time = proj['attributes']['start_time']
            stat_type = proj['attributes']['stat_type']
            status = proj['attributes']['status']

            # Track which projections have been updated for each player
            if player_id not in updated_projection_ids:
                updated_projection_ids[player_id] = set()
            updated_projection_ids[player_id].add(projection_id)

            # Check if player exists in the local cache (prizepicks_nfl_cache)
            # need to change this to a bool it either exist or not don't need to get the data right now
            player_info = self.redis_manager.get_player(player_id)

            # Case 1: If player doesn't exist in cache, it's a new player
            if not player_info:
                new_player_set.add(player_id)  # Track the new player

                # Add this new projection for the player to the filtered projections dictionary
                if player_id not in filtered_projections:
                    filtered_projections[player_id] = {}

                filtered_projections[player_id][projection_id] = {
                    "line_score": line_score,
                    "stat_type": stat_type,
                    "start_time": start_time,
                    "status": status
                }

            # Case 2: If the player exists in the cache, there are two possible scenarios:
            else:
                # Retrieve the existing projection for this player (if it exists in cache)
                cached_projection = player_info.get('projections', {}).get(projection_id)

                # Scenario 2a: Player exists in cache, but the projection is not found (new stat added)
                if not cached_projection:
                    existing_player_with_newProjection_set.add(player_id)  # Track player with new projection

                    # Add this new projection to the filtered projections
                    if player_id not in filtered_projections:
                        filtered_projections[player_id] = {}

                    filtered_projections[player_id][projection_id] = {
                        "line_score": line_score,
                        "stat_type": stat_type,
                        "start_time": start_time,
                        "status": status
                    }

                # Scenario 2b: Player exists and the projection is found, check if it was modified
                else:
                    # Compare the fields of the current projection with the cached projection to detect any changes
                    changed_fields = self.detect_field_changes(proj['attributes'], cached_projection)

                    # If changes were detected, this projection has been modified, so we flag it for update
                    if changed_fields:
                        existing_player_with_changedProjection_set.add(player_id)  # Track player with changed projection

                        # Add the updated projection to the filtered projections
                        if player_id not in filtered_projections:
                            filtered_projections[player_id] = {}

                        filtered_projections[player_id][projection_id] = {
                            "line_score": line_score,
                            "stat_type": stat_type,
                            "start_time": start_time,
                            "status": status,
                            "changed_fields": changed_fields  # Include details of the changed fields
                        }

        # Log the number of new players, existing players with new projections, and existing players with changed projections
        logging.info(f"{len(new_player_set)} new players, {len(existing_player_with_newProjection_set)} existing players with new projections, {len(existing_player_with_changedProjection_set)} existing players with changed projections.")

        # Remove any outdated projections that are no longer present in the latest API response
        self.remove_outdated_projections(updated_projection_ids)

        # Return the filtered projections (new or updated)
        return filtered_projections


    def detect_field_changes(self, projection, cached_projection):
        """
        Detect which specific fields have changed between the projection and cached projection.
        """
        changed_fields = {}
        fields_to_check = ["line_score", "start_time", "status"]

        for field in fields_to_check:
            if projection[field] != cached_projection.get(field):
                logging.info(f"Field '{field}' changed: old value = {cached_projection.get(field)}, new value = {projection[field]}")
                changed_fields[field] = {
                    "old": cached_projection.get(field),
                    "new": projection[field]
                }
            else:
                logging.debug(f"Field '{field}' unchanged: value = {projection[field]}")

        if changed_fields:
            logging.info(f"Detected field changes: {changed_fields}")
            return changed_fields
        else:
            return None  # Return None if no fields changed
