import logging
import time
import asyncio
from typing import Dict, Set, Optional
from collections import defaultdict
from managers.firebase_manager import FirebaseManager
from managers.cache_manager import CacheManager
from utils.data_fetcher import DataFetcher


class ProjectionProcessor:
    """
    Processes player projections, manages consistency checks, and updates Firebase.
    """

    def __init__(self, cache_manager: CacheManager, firebase_manager: FirebaseManager, data_fetcher: DataFetcher, platform_abbr: str):
        """
        Initialize the ProjectionProcessor.

        Args:
            cache_manager (CacheManager): Handles Redis caching.
            firebase_manager (FirebaseManager): Manages Firebase interactions.
            data_fetcher (DataFetcher): Fetches data from external APIs.
            platform_abbr (str): Abbreviation for the platform (e.g., "pp" for PrizePicks).
        """
        self.cache_manager = cache_manager
        self.firebase_manager = firebase_manager
        self.data_fetcher = data_fetcher
        self.platform_abbr = platform_abbr

    @staticmethod
    def is_player_info_complete(player_info: dict) -> bool:
        """
        Check if player metadata contains all required fields.
        Helper function because when I initialy made the playerdb I didnt include all the fields 
        that I needed so this is my way of fixing it overtime.
        Args:
            player_info (dict): Player metadata.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        required_fields = ["name", "position", "team", "league"]
        image_url = player_info.get("image_url")
        return all(player_info.get(field) for field in required_fields) and image_url is not None

    async def process_projections(self, projections: dict, ref_path: str) -> dict:
        """
        Process player projections, determine top-level consistency, and upload changes to Firebase.

        Args:
            projections (dict): Projections grouped by player ID.
            ref_path (str): Firebase reference path for projections.

        Returns:
            dict: Remaining projections for players with incomplete metadata.
        """
        logging.info("[process_projections] Starting projection processing...")
        players_with_changes = {}
        remaining_projections = {}

        try:
            logging.debug(f"[process_projections] Total projections to process: {len(projections)}")
            league_abbr = self.firebase_manager._extract_league_from_ref(ref_path)

            # Collect unique game IDs for batch fetching
            game_ids = {details.get("game_id") for proj_data in projections.values() for details in proj_data.values() if details.get("game_id")}
            logging.info(f"[process_projections] Found {len(game_ids)} unique game IDs.")

            # Fetch game information
            game_info_data = await self.data_fetcher.fetch_game_info(list(game_ids))
            game_mapping = {game['attributes']['external_game_id']: game['attributes']['metadata'] for game in game_info_data}

            for player_id, proj_data in projections.items():
                logging.debug(f"[process_projections] Processing player ID: {player_id}")

                # Fetch player info from cache
                player_info = self.cache_manager.get_player(player_id)
                if not self.is_player_info_complete(player_info or {}):
                    logging.info(f"[process_projections] Player {player_id} has incomplete metadata. Deferring...")
                    remaining_projections[player_id] = proj_data
                    continue

                # Initialize player-level data
                if player_id not in players_with_changes:
                    players_with_changes[player_id] = {
                        "name": player_info.get("name"),
                        "position": player_info.get("position"),
                        "team": player_info.get("team"),
                        "league": player_info.get("league"),
                        "image_url": player_info.get("image_url"),
                        "game_id": None,
                        "home_or_away": None,
                        "opponent": None,
                        "projections": {},
                        "inconsistent_projections": False  # Add flag for inconsistent projections
                    }

                # Track top-level consistency
                top_level_game_id = None
                top_level_home_or_away = None
                top_level_opponent = None
                consistent_top_level = True

                # Process each projection
                for proj_id, details in proj_data.items():
                    game_id = details.get("game_id")
                    game_info = game_mapping.get(game_id, {})
                    team_abbr = player_info.get("team")
                    home_or_away = "unknown"
                    opponent = "unknown"

                    # Determine home_or_away and opponent
                    if game_info:
                        teams = game_info.get("game_info", {}).get("teams", {})
                        home_team = teams.get("home", {}).get("abbreviation")
                        away_team = teams.get("away", {}).get("abbreviation")

                        if team_abbr == home_team:
                            home_or_away = "home"
                            opponent = away_team
                        elif team_abbr == away_team:
                            home_or_away = "away"
                            opponent = home_team

                    # Check for consistency
                    if top_level_game_id is None:
                        top_level_game_id = game_id
                        top_level_home_or_away = home_or_away
                        top_level_opponent = opponent
                    elif (
                        game_id != top_level_game_id
                        or home_or_away != top_level_home_or_away
                        or opponent != top_level_opponent
                    ):
                        consistent_top_level = False

                    # Add projection-level data
                    details["home_or_away"] = home_or_away
                    details["opponent"] = opponent
                    players_with_changes[player_id]["projections"][proj_id] = details

                # Move consistent data to top level
                if consistent_top_level:
                    player_changes = players_with_changes[player_id]
                    player_changes["game_id"] = top_level_game_id
                    player_changes["home_or_away"] = top_level_home_or_away
                    player_changes["opponent"] = top_level_opponent

                    # Remove redundant data
                    for proj_id, details in player_changes["projections"].items():
                        details.pop("home_or_away", None)
                        details.pop("opponent", None)
                        details.pop("game_id", None)
                else:
                    # Mark the player as having inconsistent projections
                    players_with_changes[player_id]["inconsistent_projections"] = True

            # Log summary of changes
            if players_with_changes:
                logging.info(f"[process_projections] Processed {len(players_with_changes)} players with changes.")
                first_player_id = next(iter(players_with_changes))
                logging.debug(f"[process_projections] Example player data: {players_with_changes[first_player_id]}")

                # Upload changes to Firebase
                self.firebase_manager.update_projections(players_with_changes, ref_path)

            logging.info("[process_projections] Projection processing complete.")
            return remaining_projections

        except Exception as e:
            logging.error(f"[process_projections] Exception occurred: {e}", exc_info=True)
            return {}

    async def fetch_remaining_players(self, remaining_projections: dict, ref_path: str) -> None:
        """
        Fetch missing player metadata, upload it to Firebase and cache, and reprocess projections.

        Args:
            remaining_projections (dict): Projections for players with incomplete metadata.
            ref_path (str): Firebase reference path for projections.
        """
        logging.info("[fetch_remaining_players] Fetching missing player data...")

        try:
            for player_id, proj_data in remaining_projections.items():
                try:
                    # Fetch player data from external API
                    raw_data = await self.data_fetcher.fetch_player_data(player_id)
                    logging.debug(f"[fetch_remaining_players] Raw data for {player_id}: {raw_data}")

                    player_data = raw_data.get("player_data") if isinstance(raw_data, dict) else None

                    if player_data and isinstance(player_data, dict):
                        # Prepare player metadata
                        player_metadata = {
                            "name": player_data.get("name") or player_data.get("display_name"),
                            "position": player_data.get("position"),
                            "team": player_data.get("team"),
                            "league": player_data.get("league"),
                            "image_url": player_data.get("image_url") or "__missing__",
                        }

                        # Upload player metadata to Firebase and cache
                        self.firebase_manager.update_player(player_id, player_metadata)
                        self.cache_manager.set_player(player_id, player_metadata)
                    else:
                        logging.warning(f"[fetch_remaining_players] No valid player_data found for {player_id}")

                except Exception as e:
                    logging.error(f"[fetch_remaining_players] Error fetching player {player_id}: {e}")

            # After fetching metadata, reprocess projections
            logging.info("[fetch_remaining_players] All missing metadata fetched. Reprocessing projections...")
            await self.process_projections(remaining_projections, ref_path)

        except Exception as e:
            logging.error(f"[fetch_remaining_players] Exception occurred: {e}", exc_info=True)

    async def remove_outdated_projections(self, current_projection_map: Dict[str, Set[str]], ref_path: str) -> None:
        """
        Remove outdated projections and player nodes from Firebase.

        Args:
            current_projection_map (Dict[str, Set[str]]): Map of current projections by player ID.
            ref_path (str): Firebase reference path for projections.
        """
        logging.info("[remove_outdated_projections] Checking for outdated projections...")
        projections_to_remove = []
        players_to_remove_entirely = []
        league = self.firebase_manager._extract_league_from_ref(ref_path)

        try:
            # Retrieve cached player projections
            cached_players = self.cache_manager.get_all_player_projections_by_league(league)

            for player_id, cached_data in cached_players.items():
                cached_proj_data = cached_data.get("projections", {})
                cached_proj_ids = set(cached_proj_data.keys())
                current_proj_ids = current_projection_map.get(player_id)

                # Case 1 & 3: Player not in API response OR has no projections left
                if not current_proj_ids:
                    players_to_remove_entirely.append(player_id)
                    logging.info(f"[remove_outdated_projections] Player {player_id} has no remaining projections. Marked for full projection node removal.")
                    continue

                # Case 2: Player still exists, but some projections are outdated
                to_remove = [pid for pid in cached_proj_ids if pid not in current_proj_ids]
                if to_remove:
                    logging.info(f"[remove_outdated_projections] Player {player_id} projections to remove: {to_remove}")
                    projections_to_remove.extend([(player_id, pid) for pid in to_remove])

            # Perform deletions
            if projections_to_remove:
                self.firebase_manager.delete_projections(projections_to_remove, ref_path)
                logging.info(
                    f"[remove_outdated_projections] ✅ Deleted {len(projections_to_remove)} projections "
                    f"from {len(set(p for p, _ in projections_to_remove))} players."
                )

            if players_to_remove_entirely:
                self.firebase_manager.delete_entire_player_nodes(players_to_remove_entirely, ref_path)
                logging.info(
                    f"[remove_outdated_projections] 🚫 Removed {len(players_to_remove_entirely)} full player projection nodes."
                )

            if not projections_to_remove and not players_to_remove_entirely:
                logging.info("[remove_outdated_projections] No projections to remove.")

        except Exception as e:
            logging.error(f"[remove_outdated_projections] Error: {e}")

    def filter_relevant_projections(self, projections: list, ref_path: str) -> dict:
        """
        Filter projections to identify new, changed, or relevant projections.

        Args:
            projections (list): List of projections from the external API.
            ref_path (str): Firebase reference path for projections.

        Returns:
            dict: Filtered projections grouped by player ID.
        """
        logging.info("[filter_relevant_projections] Filtering projections...")
        filtered_projections = {}
        active_projection_map: Dict[str, Set[str]] = defaultdict(set)

        new_player_set = set()
        changed_proj_set = set()
        new_proj_set = set()

        try:
            league = self.firebase_manager._extract_league_from_ref(ref_path)

            for proj in projections:
                player_id = proj['relationships']['new_player']['data']['id']
                projection_id = proj['id']

                line_score = proj['attributes']['line_score']
                start_time = proj['attributes']['start_time']
                stat_type = proj['attributes']['stat_type']
                status = proj['attributes']['status']
                game_id = proj['attributes'].get('game_id')  # Add game_id here

                active_projection_map[player_id].add(projection_id)

                cached_proj_data = self.cache_manager.get_projection_by_league(
                    player_id, league
                )
                cached_projection = cached_proj_data.get("projections", {}).get(projection_id) if cached_proj_data else None

                if not cached_projection:
                    new_proj_set.add(player_id)
                    filtered_projections.setdefault(player_id, {})[projection_id] = {
                        "line_score": line_score,
                        "stat_type": stat_type,
                        "start_time": start_time,
                        "status": status,
                        "game_id": game_id  # Include game_id in the projection data
                    }
                else:
                    changed_fields = self.detect_field_changes(proj['attributes'], cached_projection)
                    if changed_fields:
                        changed_proj_set.add(player_id)
                        filtered_projections.setdefault(player_id, {})[projection_id] = {
                            "line_score": line_score,
                            "stat_type": stat_type,
                            "start_time": start_time,
                            "status": status,
                            "game_id": game_id,  # Include game_id in the projection data
                            "changed_fields": changed_fields
                        }

            logging.info(
                f"[filter_relevant_projections] New: {len(new_proj_set)}, Changed: {len(changed_proj_set)}"
            )

            asyncio.create_task(
                self.remove_outdated_projections(active_projection_map, ref_path)
            )

            return filtered_projections

        except Exception as e:
            logging.error(f"[filter_relevant_projections] Error: {e}")
            return {}

    def detect_field_changes(self, projection: dict, cached_projection: dict) -> Optional[dict]:
        """
        Detect changes in projection fields compared to cached data.

        Args:
            projection (dict): Current projection data.
            cached_projection (dict): Cached projection data.

        Returns:
            Optional[dict]: Dictionary of changed fields, or None if no changes.
        """
        changed_fields = {}
        try:
            for field in ["line_score", "start_time", "status"]:
                if projection[field] != cached_projection.get(field):
                    logging.info(f"[detect_field_changes] Field '{field}' changed: {cached_projection.get(field)} → {projection[field]}")
                    changed_fields[field] = {
                        "old": cached_projection.get(field),
                        "new": projection[field]
                    }
        except Exception as e:
            logging.error(f"[detect_field_changes] Error comparing fields: {e}")
        return changed_fields if changed_fields else None

