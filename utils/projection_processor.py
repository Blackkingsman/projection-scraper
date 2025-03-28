import logging
import time
import asyncio
from typing import Dict, Set

from managers.firebase_manager import FirebaseManager
from managers.cache_manager import CacheManager
from utils.data_fetcher import DataFetcher


class ProjectionProcessor:
    def __init__(self, cache_manager: CacheManager, firebase_manager: FirebaseManager, data_fetcher: DataFetcher, platform_abbr: str):
        self.cache_manager = cache_manager
        self.firebase_manager = firebase_manager
        self.data_fetcher = data_fetcher
        self.platform_abbr = platform_abbr

    def process_projections(self, projections, ref_path: str):
        logging.info("[process_projections] Starting to process projections...")
        players_with_changes = {}
        remaining_projections = {}

        try:
            for player_id, proj_data in projections.items():
                player_info = self.cache_manager.get_player(player_id)

                filtered_proj_data = {}
                for proj_id, details in proj_data.items():
                    filtered_details = {
                        "line_score": details.get("line_score"),
                        "start_time": details.get("start_time"),
                        "stat_type": details.get("stat_type"),
                        "status": details.get("status")
                    }
                    filtered_proj_data[proj_id] = filtered_details

                if not player_info:
                    logging.info(f"[process_projections] Player {player_id} not found in cache.")
                    remaining_projections[player_id] = filtered_proj_data
                    continue

                for field in ["created_at", "timestamp", "version", "league_id", "market",
                              "oddsjam_id", "team_name", "updated_at", "prizepicks_updated_at"]:
                    player_info.pop(field, None)

                for proj_id, details in filtered_proj_data.items():
                    if "projections" not in player_info:
                        player_info["projections"] = {}

                    if proj_id not in player_info["projections"] or player_info["projections"][proj_id] != details:
                        player_info["projections"][proj_id] = details
                        logging.info(f"[process_projections] Player {player_id} - Projection {proj_id} flagged for update.")

                        if player_id not in players_with_changes:
                            players_with_changes[player_id] = {
                                "name": player_info.get("name"),
                                "position": player_info.get("position"),
                                "team": player_info.get("team"),
                                "league": player_info.get("league"),
                                "projections": {},
                                "jersey_number": player_info.get("jersey_number"),
                                "image_url": player_info.get("image_url")
                            }
                        players_with_changes[player_id]["projections"][proj_id] = details

            if players_with_changes:
                logging.info(f"[process_projections] Uploading {len(players_with_changes)} changed players.")
                self.firebase_manager.update_projections(players_with_changes, ref_path)

            return remaining_projections

        except Exception as e:
            logging.error(f"[process_projections] Exception: {e}")
            return {}

    async def fetch_remaining_players(self, remaining_projections, ref_path: str):
        logging.info("[fetch_remaining_players] Fetching missing player data...")
        players_with_changes = {}

        for player_id, proj_data in remaining_projections.items():
            try:
                raw_data = await self.data_fetcher.fetch_player_data(player_id)
                logging.debug(f"[fetch_remaining_players] Raw data for {player_id}: {raw_data}")

                player_data = raw_data.get("player_data") if isinstance(raw_data, dict) else None

                if player_data and isinstance(player_data, dict):
                    name = player_data.get("name") or player_data.get("display_name")
                    position = player_data.get("position")
                    team = player_data.get("team")
                    league = player_data.get("league")
                    image_url = player_data.get("image_url")
                    jersey_number = player_data.get("jersey_number")

                    merged_player_data = {
                        "name": name,
                        "position": position,
                        "team": team,
                        "league": league,
                        "jersey_number": jersey_number,
                        "image_url": image_url,
                        "projections": proj_data
                    }

                    players_with_changes[player_id] = merged_player_data

                    filtered_player_info = {
                        "name": name,
                        "position": position,
                        "team": team,
                        "league": league,
                        "image_url": image_url,
                        "jersey_number": jersey_number
                    }

                    logging.info(f"[update_player] Data for {player_id} => {filtered_player_info}")
                    self.firebase_manager.update_player(player_id, filtered_player_info)

                else:
                    logging.warning(f"[fetch_remaining_players] No valid player_data found for {player_id}")

            except Exception as e:
                logging.error(f"[fetch_remaining_players] Error fetching player {player_id}: {e}")

        if players_with_changes:
            logging.info(f"[fetch_remaining_players] Uploading {len(players_with_changes)} players to Firebase projections ref.")
            self.firebase_manager.update_projections(players_with_changes, ref_path)

    async def remove_outdated_projections(self, current_projection_map: Dict[str, Set[str]], ref_path: str):
        logging.info("[remove_outdated_projections] Checking for outdated projections...")
        projections_to_remove = []
        total_players_checked = 0
        total_projections_removed = 0
        league = self.firebase_manager._extract_league_from_ref(ref_path)

        try:
            for player_id, current_ids in current_projection_map.items():
                cached_data = self.cache_manager.get_projection_by_league(player_id, self.platform_abbr, league)
                if not cached_data or "projections" not in cached_data:
                    continue

                total_players_checked += 1
                cached_ids = set(cached_data["projections"].keys())

                for projection_id in cached_ids:
                    if projection_id not in current_ids:
                        projections_to_remove.append((player_id, projection_id))
                        total_projections_removed += 1

            if projections_to_remove:
                self.firebase_manager.delete_projections(projections_to_remove, ref_path)
                logging.info(f"[remove_outdated_projections] Deleted {total_projections_removed} projections from {total_players_checked} players.")
            else:
                logging.info("[remove_outdated_projections] No projections to remove.")

        except Exception as e:
            logging.error(f"[remove_outdated_projections] Error: {e}")

    def filter_relevant_projections(self, projections, ref_path: str):
        logging.info("[filter_relevant_projections] Filtering projections...")
        filtered_projections = {}
        updated_projection_ids: Dict[str, Set[str]] = {}

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

                updated_projection_ids.setdefault(player_id, set()).add(projection_id)

                cached_proj_data = self.cache_manager.get_projection_by_league(
                    player_id, self.platform_abbr, league
                )
                cached_projection = cached_proj_data.get("projections", {}).get(projection_id) if cached_proj_data else None

                if not cached_projection:
                    new_proj_set.add(player_id)
                    filtered_projections.setdefault(player_id, {})[projection_id] = {
                        "line_score": line_score,
                        "stat_type": stat_type,
                        "start_time": start_time,
                        "status": status
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
                            "changed_fields": changed_fields
                        }

            logging.info(
                f"[filter_relevant_projections] New: {len(new_proj_set)}, Changed: {len(changed_proj_set)}"
            )

            asyncio.create_task(
                self.remove_outdated_projections(updated_projection_ids, ref_path)
            )

            return filtered_projections

        except Exception as e:
            logging.error(f"[filter_relevant_projections] Error: {e}")
            return {}

    def detect_field_changes(self, projection, cached_projection):
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

