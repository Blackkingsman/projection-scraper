import logging
import time
import asyncio
from typing import Dict, Set, Optional
from collections import defaultdict
import datetime
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
                        "start_time": None,
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
                    start_time = details.get("start_time")
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
                        top_level_start_time = start_time
                    elif (
                        game_id != top_level_game_id
                        or home_or_away != top_level_home_or_away
                        or opponent != top_level_opponent
                    ):
                        consistent_top_level = False

                    # Add projection-level data
                    details["home_or_away"] = home_or_away
                    details["opponent"] = opponent
                    details["start_time"] = start_time
                    players_with_changes[player_id]["projections"][proj_id] = details

                # Move consistent data to top level
                if consistent_top_level:
                    player_changes = players_with_changes[player_id]
                    player_changes["game_id"] = top_level_game_id
                    player_changes["home_or_away"] = top_level_home_or_away
                    player_changes["opponent"] = top_level_opponent
                    player_changes["start_time"] = top_level_start_time

                    # Remove redundant data
                    for proj_id, details in player_changes["projections"].items():
                        details.pop("home_or_away", None)
                        details.pop("opponent", None)
                        details.pop("game_id", None)
                        details.pop("start_time", None)
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
            # Retrieve cached player projections with thread safety
            cached_players = self.cache_manager.get_all_player_projections_by_league(league)
            
            # Debug A.J. Brown cache status
            if "206304" in cached_players:
                aj_cache = cached_players["206304"]
                aj_proj_ids = set(aj_cache.get("projections", {}).keys())
                logging.info(f"[AJ_BROWN_DEBUG] A.J. Brown in cache with projections: {aj_proj_ids} (total: {len(aj_proj_ids)})")
            else:
                logging.warning(f"[AJ_BROWN_DEBUG] A.J. Brown (206304) NOT found in cache!")
            
            # Validate current_projection_map type
            if not isinstance(current_projection_map, dict):
                logging.error(f"[remove_outdated_projections] Invalid current_projection_map type: {type(current_projection_map)}")
                return

            for player_id, cached_data in cached_players.items():
                if not isinstance(cached_data, dict):
                    logging.warning(f"[remove_outdated_projections] Invalid cached_data for player {player_id}: {type(cached_data)}")
                    continue
                    
                cached_proj_data = cached_data.get("projections", {})
                if not isinstance(cached_proj_data, dict):
                    logging.warning(f"[remove_outdated_projections] Invalid projections data for player {player_id}: {type(cached_proj_data)}")
                    continue
                    
                cached_proj_ids = set(cached_proj_data.keys())
                current_proj_ids = current_projection_map.get(player_id, set())
                
                # Debug A.J. Brown comparison
                if player_id == "206304":
                    logging.info(f"[AJ_BROWN_DEBUG] Comparison - cached_proj_ids: {cached_proj_ids}, current_proj_ids: {current_proj_ids}")
                
                # Ensure current_proj_ids is a set
                if not isinstance(current_proj_ids, set):
                    logging.warning(f"[remove_outdated_projections] Converting current_proj_ids to set for player {player_id}")
                    current_proj_ids = set(current_proj_ids) if current_proj_ids else set()

                # Case 1 & 3: Player not in API response OR has no projections left
                if not current_proj_ids:
                    # SAFETY CHECK: For A.J. Brown, this should NEVER happen during normal operation
                    if player_id == "206304":
                        logging.error(f"[AJ_BROWN_DEBUG] CRITICAL: A.J. Brown marked for full removal! This should NOT happen if projections exist on PrizePicks")
                        logging.error(f"[AJ_BROWN_DEBUG] Cached projections: {cached_proj_ids}, Current from API: {current_proj_ids}")
                        # Don't remove A.J. Brown entirely unless we're sure
                        continue
                    
                    players_to_remove_entirely.append(player_id)
                    logging.info(f"[remove_outdated_projections] Player {player_id} has no remaining projections. Marked for full projection node removal.")
                    continue

                # Case 2: Player still exists, but some projections are outdated
                to_remove = [pid for pid in cached_proj_ids if pid not in current_proj_ids]
                if to_remove:
                    # SAFETY CHECK: For A.J. Brown, validate removal is reasonable
                    if player_id == "206304":
                        remaining_count = len(current_proj_ids)
                        logging.info(f"[AJ_BROWN_DEBUG] A.J. Brown partial removal - cached: {cached_proj_ids}, current: {current_proj_ids}, removing: {to_remove}, remaining: {remaining_count}")
                        
                        # Safety: Don't allow A.J. Brown to drop below 3 projections unless legitimately low
                        if remaining_count < 3 and len(cached_proj_ids) >= 5:
                            logging.error(f"[AJ_BROWN_DEBUG] SAFETY VIOLATION: A.J. Brown would drop from {len(cached_proj_ids)} to {remaining_count} projections. Blocking removal!")
                            continue
                    
                    logging.info(f"[remove_outdated_projections] Player {player_id} projections to remove: {to_remove}")
                    projections_to_remove.extend([(player_id, pid) for pid in to_remove])

            # Perform deletions with error handling
            if projections_to_remove:
                try:
                    self.firebase_manager.delete_projections(projections_to_remove, ref_path)
                    logging.info(
                        f"[remove_outdated_projections] ✅ Deleted {len(projections_to_remove)} projections "
                        f"from {len(set(p for p, _ in projections_to_remove))} players."
                    )
                except Exception as e:
                    logging.error(f"[remove_outdated_projections] Error deleting projections: {e}")

            if players_to_remove_entirely:
                try:
                    self.firebase_manager.delete_entire_player_nodes(players_to_remove_entirely, ref_path)
                    logging.info(
                        f"[remove_outdated_projections] 🚫 Removed {len(players_to_remove_entirely)} full player projection nodes."
                    )
                except Exception as e:
                    logging.error(f"[remove_outdated_projections] Error deleting player nodes: {e}")

            if not projections_to_remove and not players_to_remove_entirely:
                logging.info("[remove_outdated_projections] No projections to remove.")

        except Exception as e:
            logging.error(f"[remove_outdated_projections] Error: {e}", exc_info=True)

    async def store_historical_projections(self, current_projection_map: Dict[str, Set[str]], ref_path: str, historical_ref_path: str, changed_map: Optional[Dict[str, Dict]] = None) -> None:
        """
        Store historical projections in Firebase under a timestamp-based structure.

        Args:
            current_projection_map: Map of current projections by player ID.
            ref_path: Firebase reference path for current projections.
            historical_ref_path: Firebase reference path for historical projections.
        """
        logging.info("[store_historical_projections] Storing historical projections...")
        try:
            logging.debug(f"[store_historical_projections] Current projection map: {current_projection_map}")
            changed_map = changed_map or {}
            historical_data = {}
            league = self.firebase_manager._extract_league_from_ref(ref_path)
            cached_players = self.cache_manager.get_all_player_projections_by_league(league)
            for player_id, cached_data in cached_players.items():
                logging.debug(f"[store_historical_projections] Processing player_id: {player_id}, Cached data type: {type(cached_data)}")
                proj_data = cached_data.get("projections", {})
                current_ids = current_projection_map.get(player_id)

                # Case 1: player gone completely
                if not current_ids:
                    logging.info(f"[store_historical_projections] Player {player_id} gone → archiving full node")
                    historical_data[player_id] = cached_data
                    continue

                # Case 2: some projections removed/changed
                removed = {pid for pid in proj_data if pid not in current_ids}
                changed_pids = set(changed_map.get(player_id, {}).keys())
                target_ids = removed.union(changed_pids)
                logging.debug(f"[store_historical_projections] Target IDs for player {player_id}: {target_ids}")

                if target_ids:
                    to_archive = {}
                    for pid in target_ids:
                        p = proj_data.get(pid)
                        if not p:
                            continue

                        # Build line_score_history based on the situation
                        history = []
                        
                        # If this projection changed and line_score changed, use the history from filter_relevant_projections
                        if pid in changed_pids:
                            entry = changed_map[player_id][pid]
                            history = entry.get('line_score_history', [])
                            changes = entry.get('changed_fields', {})
                            if 'line_score' in changes:
                                logging.info(f"[store_historical_projections] Line score changed for player {player_id}, projection {pid}: {changes['line_score']['old']} → {changes['line_score']['new']}")
                        
                        # If projection is being removed and we don't have any history yet, capture the last known line_score
                        if pid in removed and not history and 'line_score' in p:
                            history = [p['line_score']]
                            logging.info(f"[store_historical_projections] Capturing last known line_score for removed projection {pid}: {p['line_score']}")

                        # Build archive entry
                        archive_entry = {**p}
                        archive_entry['line_score_history'] = history
                        archive_entry.pop('line_score', None)  # Remove line_score from archive
                        
                        if pid in changed_pids:
                            archive_entry['changed_fields'] = changed_map[player_id][pid].get('changed_fields', {})

                        logging.info(f"[store_historical_projections] Archiving player {player_id}, projection {pid} with line_score_history: {history}")
                        to_archive[pid] = archive_entry

                    if to_archive:
                        # Preserve existing player metadata and enrich projections
                        historical_data[player_id] = {**cached_data, "projections": to_archive}

            if not historical_data:
                logging.warning("[store_historical_projections] No historical data to write despite detected changes.")
                return

            # pick date key
            first = next(iter(historical_data.values()))
            ts = first.get('start_time') or next((p.get('date_key') for p in first['projections'].values() if p.get('date_key')), None)
            parsed = datetime.datetime.fromisoformat(ts)
            readable_date = parsed.strftime("%d%b%Y").upper()
            # archive
            hist_ref = self.firebase_manager._get_projection_ref(historical_ref_path)
            hist_ref.child(readable_date).update(historical_data)
            logging.info(f"[store_historical_projections] Archived under '{readable_date}' → {len(historical_data)} players.")
        except Exception as e:
            logging.error(f"[store_historical_projections] Failed: {e}", exc_info=True)

    async def filter_relevant_projections(self, projections: list, ref_path: str) -> dict:
        """
        Filter projections to identify new, changed, or relevant projections.

        Args:
            projections (list): List of projections from the external API.
            ref_path (str): Firebase reference path for projections.

        Returns:
            dict: Filtered projections grouped by player ID.
        """
        logging.info("[filter_relevant_projections] Filtering projections...")
        
        # Debug A.J. Brown at the very start
        aj_brown_count = len([p for p in projections if p['relationships']['new_player']['data']['id'] == "206304"])
        if aj_brown_count > 0:
            logging.info(f"[AJ_BROWN_DEBUG] Found {aj_brown_count} A.J. Brown projections in raw API response")
            aj_brown_proj_ids = [p['id'] for p in projections if p['relationships']['new_player']['data']['id'] == "206304"]
            logging.info(f"[AJ_BROWN_DEBUG] A.J. Brown projection IDs in API: {aj_brown_proj_ids}")
        else:
            logging.warning(f"[AJ_BROWN_DEBUG] A.J. Brown (206304) NOT found in API response! Total projections: {len(projections)}")
        
        try:
            logging.debug(f"[filter_relevant_projections] Projections type: {type(projections)}, Content: {projections}")
            filtered_projections = {}
            active_projection_map: Dict[str, Set[str]] = defaultdict(set)

            new_player_set = set()
            changed_proj_set = set()                                                                                      
            new_proj_set = set()

            league = self.firebase_manager._extract_league_from_ref(ref_path)

            # First pass: Build active projection map and filter projections
            for proj in projections:
                player_id = proj['relationships']['new_player']['data']['id']
                projection_id = proj['id']
                logging.debug(f"[filter_relevant_projections] Processing player_id: {player_id}, projection_id: {projection_id}")

                # Debug A.J. Brown specifically
                if player_id == "206304":
                    logging.info(f"[AJ_BROWN_DEBUG] Found A.J. Brown projection {projection_id} in API response")

                line_score = proj['attributes']['line_score']
                start_time = proj['attributes']['start_time']
                stat_type = proj['attributes']['stat_type']
                status = proj['attributes']['status']
                game_id = proj['attributes'].get('game_id')  # Add game_id here

                # Extract date from start_time
                try:
                    parsed_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z")
                    date_key = parsed_time.strftime("%Y-%m-%d")
                except ValueError as ve:
                    logging.error(f"[filter_relevant_projections] Invalid start_time format for projection {projection_id}: {start_time}. Error: {ve}")
                    # Debug A.J. Brown date parsing issues
                    if player_id == "206304":
                        logging.error(f"[AJ_BROWN_DEBUG] Date parsing failed for A.J. Brown projection {projection_id}")
                    continue

                # Track active projections
                active_projection_map[player_id].add(projection_id)
                
                # Debug A.J. Brown active projections
                if player_id == "206304":
                    logging.info(f"[AJ_BROWN_DEBUG] Added to active_projection_map. Total for A.J. Brown: {len(active_projection_map[player_id])}")

                cached_proj_data = self.cache_manager.get_projection_by_league(
                    player_id, league
                )
                cached_projection = cached_proj_data.get("projections", {}).get(projection_id) if cached_proj_data else None
                
                # Debug A.J. Brown cache lookup
                if player_id == "206304":
                    if cached_proj_data:
                        cached_count = len(cached_proj_data.get("projections", {}))
                        logging.info(f"[AJ_BROWN_DEBUG] Found {cached_count} cached projections for A.J. Brown")
                        if cached_projection:
                            logging.info(f"[AJ_BROWN_DEBUG] Projection {projection_id} found in cache")
                        else:
                            logging.info(f"[AJ_BROWN_DEBUG] Projection {projection_id} NOT found in cache (new projection)")
                    else:
                        logging.info(f"[AJ_BROWN_DEBUG] No cached data found for A.J. Brown")

                if not cached_projection:
                    new_proj_set.add(player_id)
                    filtered_projections.setdefault(player_id, {})[projection_id] = {
                        "line_score": line_score,
                        "stat_type": stat_type,
                        "status": status,
                        "game_id": game_id,  # Include game_id in the projection data
                        "start_time": start_time,  # Include raw start_time
                        "date_key": date_key  # Include extracted date
                    }
                    # Debug A.J. Brown new projections
                    if player_id == "206304":
                        logging.info(f"[AJ_BROWN_DEBUG] New projection {projection_id} added to filtered_projections")
                else:
                    changed_fields = self.detect_field_changes(proj['attributes'], cached_projection)
                    if changed_fields:
                        logging.info(f"[filter_relevant_projections] Detected field changes for player {player_id}, projection {projection_id}: {changed_fields}")
                        changed_proj_set.add(player_id)
                        # Build history for line_score if changed
                        history = None
                        if 'line_score' in changed_fields:
                            history = [changed_fields['line_score']['old'], changed_fields['line_score']['new']]
                        else:
                            # Even if line_score didn't change, we need to capture the current line_score for history
                            history = [line_score] if line_score is not None else None
                        filtered_projections.setdefault(player_id, {})[projection_id] = {
                            "line_score": line_score,
                            "stat_type": stat_type,
                            "status": status,
                            "game_id": game_id,  # Include game_id in the projection data
                            "start_time": start_time,  # Include raw start_time
                            "changed_fields": changed_fields,
                            "date_key": date_key,  # Include extracted date
                            # Include optional history list
                            "line_score_history": history if history is not None else cached_projection.get('line_score_history')
                        }
                        # Debug A.J. Brown changed projections
                        if player_id == "206304":
                            logging.info(f"[AJ_BROWN_DEBUG] Changed projection {projection_id} added to filtered_projections")
                        logging.debug(f"[filter_relevant_projections] Updated projection for player {player_id}, projection_id {projection_id}: {filtered_projections[player_id][projection_id]}")
                    else:
                        # Debug A.J. Brown unchanged projections (these are EXCLUDED from filtered_projections!)
                        if player_id == "206304":
                            logging.info(f"[AJ_BROWN_DEBUG] Projection {projection_id} unchanged - NOT included in filtered_projections")

            logging.info(
                f"[filter_relevant_projections] New: {len(new_proj_set)}, Changed: {len(changed_proj_set)}"
            )

            # Debug A.J. Brown final active projection count
            if "206304" in active_projection_map:
                logging.info(f"[AJ_BROWN_DEBUG] Final active_projection_map for A.J. Brown: {active_projection_map['206304']} (total: {len(active_projection_map['206304'])})")
            else:
                logging.warning(f"[AJ_BROWN_DEBUG] A.J. Brown (206304) NOT found in active_projection_map!")

            # Debug A.J. Brown in filtered projections
            if "206304" in filtered_projections:
                logging.info(f"[AJ_BROWN_DEBUG] A.J. Brown in filtered_projections with {len(filtered_projections['206304'])} projections")
            else:
                logging.warning(f"[AJ_BROWN_DEBUG] A.J. Brown NOT in filtered_projections (no new/changed projections)")

            # Archive historical projections and remove outdated when changes detected
            historical_ref_path = f"{ref_path}Historicals"
            # Build map of only changed entries for history
            changed_map: Dict[str, Dict[str, dict]] = {}
            for player_id, proj_dict in filtered_projections.items():
                changes = {
                    proj_id: {**entry, "projection_id": proj_id}  # Attach projection_id
                    for proj_id, entry in proj_dict.items()
                    if entry.get("changed_fields")
                }
                if changes:
                    changed_map[player_id] = changes

            # Only archive & remove when there are actual changes OR we have removed projections
            # IMPORTANT: Add safety check to prevent incorrect removals
            has_removed = self._has_removed_projections(active_projection_map, league)
            
            # Debug A.J. Brown removal detection
            if "206304" in active_projection_map and has_removed:
                # Check if A.J. Brown is flagged for removal
                cached_players = self.cache_manager.get_all_player_projections_by_league(league)
                if "206304" in cached_players:
                    aj_cached_ids = set(cached_players["206304"].get("projections", {}).keys())
                    aj_current_ids = active_projection_map["206304"]
                    aj_to_remove = aj_cached_ids - aj_current_ids
                    if aj_to_remove:
                        logging.warning(f"[AJ_BROWN_DEBUG] REMOVAL DETECTED! Cached: {aj_cached_ids}, Current: {aj_current_ids}, Would remove: {aj_to_remove}")
                    
            if changed_map or has_removed:
                # Archive historicals (handles removed entries too)
                logging.info(f"[filter_relevant_projections] Storing historical projections for {len(changed_map)} players.")
                # Use synchronous calls to prevent race conditions
                try:
                    await self.store_historical_projections(
                        active_projection_map,
                        ref_path,
                        historical_ref_path,
                        changed_map
                    )
                    # Remove outdated projections after historical storage completes
                    await self.remove_outdated_projections(
                        active_projection_map,
                        ref_path
                    )
                except Exception as e:
                    logging.error(f"[filter_relevant_projections] Error in historical processing: {e}")

            return filtered_projections

        except Exception as e:
            logging.error(f"[filter_relevant_projections] Error: {e}")
            return {}

    def _has_removed_projections(self, active_projection_map: Dict[str, Set[str]], league: str) -> bool:
        """
        Check if any projections have been removed by comparing with cached data.
        
        Args:
            active_projection_map: Current active projections from API
            league: League abbreviation
            
        Returns:
            bool: True if projections have been removed
        """
        try:
            cached_players = self.cache_manager.get_all_player_projections_by_league(league)
            
            total_removed = 0
            for player_id, cached_data in cached_players.items():
                cached_proj_ids = set(cached_data.get("projections", {}).keys())
                current_proj_ids = active_projection_map.get(player_id, set())
                
                # If any cached projections are not in current API response, we have removals
                removed_proj_ids = cached_proj_ids - current_proj_ids
                if removed_proj_ids:
                    total_removed += len(removed_proj_ids)
                    
                    # Special debug for A.J. Brown (player_id: 206304)
                    if player_id == "206304":
                        logging.warning(f"[AJ_BROWN_DEBUG] _has_removed_projections detected removal: Cached={cached_proj_ids}, Current={current_proj_ids}, ToRemove={removed_proj_ids}")
                        # Safety check: Only proceed if we have a reasonable number of current projections
                        if len(current_proj_ids) < 2:
                            logging.error(f"[AJ_BROWN_DEBUG] SAFETY VIOLATION: Only {len(current_proj_ids)} current projections for A.J. Brown! Blocking removal.")
                            return False
                    
            has_removed = total_removed > 0
            if has_removed:
                logging.info(f"[_has_removed_projections] Found {total_removed} projections to remove across all players")
            
            return has_removed
        except Exception as e:
            logging.error(f"[_has_removed_projections] Error checking for removed projections: {e}")
            return False

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
            for field in ["line_score", "stat_type", "status"]:
                if projection[field] != cached_projection.get(field):
                    logging.info(f"[detect_field_changes] Field '{field}' changed: {cached_projection.get(field)} → {projection[field]}")
                    changed_fields[field] = {
                        "old": cached_projection.get(field),
                        "new": projection[field]
                    }

            # Check start_time only if it exists in both projection and cached_projection
            if "start_time" in projection and "start_time" in cached_projection:
                if projection["start_time"] != cached_projection["start_time"]:
                    logging.info(f"[detect_field_changes] Field 'start_time' changed: {cached_projection['start_time']} → {projection['start_time']}")
                    changed_fields["start_time"] = {
                        "old": cached_projection["start_time"],
                        "new": projection["start_time"]
                    }

        except Exception as e:
            logging.error(f"[detect_field_changes] Error comparing fields: {e}")
        return changed_fields if changed_fields else None

