import json
import logging
from typing import Optional, Dict, Any
from cachetools import TTLCache

logger = logging.getLogger("CacheManager")

class CacheManager:

    def __init__(self, platform_abbr: str, maxsize: int = 500_000, ttl: int = 86400):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self.platform_abbr = platform_abbr.lower()
        logger.info(f"Initialized in-memory CacheManager for {self.platform_abbr} using cachetools")

    def get(self, key: str) -> Optional[str]:
        try:
            return self.cache.get(key)
        except Exception as e:
            logger.error(f"[get] Error retrieving key '{key}': {e}")
            return None

    # --- Player Metadata Caching ---

    def get_player(self, player_id: str) -> Optional[Dict[str, Any]]:
        try:
            key = f"{self.platform_abbr}:player:{player_id}"
            data = self.cache.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"[get_player] Error for {player_id}: {e}")
            return None

    def set_player(self, player_id: str, player_data: Dict[str, Any]) -> bool:
        try:
            key = f"{self.platform_abbr}:player:{player_id}"
            self.cache[key] = json.dumps(player_data)
            return True
        except Exception as e:
            logger.error(f"[set_player] Error setting player {player_id}: {e}")
            return False

    def remove_player(self, player_id: str):
        key = f"{self.platform_abbr}:player:{player_id}"
        if key in self.cache:
            del self.cache[key]
            logger.info(f"Removed player {player_id} from cache.")

    def player_exists(self, player_id: str) -> bool:
        key = f"{self.platform_abbr}:player:{player_id}"
        return key in self.cache

    def bulk_set_players(self, players: Dict[str, Dict[str, Any]]) -> int:
        count = 0
        for player_id, player_data in players.items():
            try:
                self.cache[f"{self.platform_abbr}:player:{player_id}"] = json.dumps(player_data)
                count += 1
            except Exception as e:
                logger.error(f"Failed to cache player {player_id}: {e}")
        return count

    def iter_player_keys(self):
        prefix = f"{self.platform_abbr}:player:"
        for key in self.cache.keys():
            if key.startswith(prefix):
                yield key

    # --- Projections Caching (by platform + league) ---

    def set_projection(self, player_id: str, player_data: Dict[str, Any], league_abbr: str) -> bool:
        """
        Stores the entire player record including projections in cache.
        """
        try:
            key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"
            self.cache[key] = json.dumps(player_data)
            return True
        except Exception as e:
            logger.error(f"[set_projection] Error for {player_id}: {e}")
            return False

    def get_projection_by_league(self, player_id: str, league_abbr: str) -> Dict[str, Any]:
        try:
            key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"
            raw = self.cache.get(key)
            return json.loads(raw) if raw else {}
        except Exception as e:
            logger.error(f"[get_projection_by_league] Error for {player_id}: {e}")
            return {}
    def clear_projections_for_league(self, league: str) -> int:
        """
        Clears all cached projections for a given league/platform combo.
        Returns the number of entries removed.
        """
        prefix = f"{self.platform_abbr}:projections:{league}:"
        keys_to_delete = [key for key in self.cache if key.startswith(prefix)]
        for key in keys_to_delete:
            del self.cache[key]
        return len(keys_to_delete)

    def get_all_player_projections_by_league(self, league_abbr: str) -> Dict[str, Dict[str, Any]]:
        """
        Retrieve all cached player projection data for a specific league.
        Keys are of format: {platform_abbr}:projections:{league_abbr}:{player_id}
        """
        result = {}
        prefix = f"{self.platform_abbr}:projections:{league_abbr.upper()}:"

        try:
            for key in self.cache.keys():
                if key.startswith(prefix):
                    try:
                        player_id = key.split(":")[-1]
                        raw = self.cache[key]
                        result[player_id] = json.loads(raw)
                    except Exception as inner_e:
                        logger.warning(f"[get_all_player_projections_by_league] Failed to parse key '{key}': {inner_e}")
        except Exception as e:
            logger.error(f"[get_all_player_projections_by_league] Error during scan: {e}")

        logger.info(f"[get_all_player_projections_by_league] Retrieved {len(result)} players from cache for {league_abbr.upper()}")
        return result

    def remove_player_projections(self, player_id: str, league_abbr: str):
        try:
            key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"
            if key in self.cache:
                del self.cache[key]
                logger.info(f"[remove_player_projections] Entire projection node removed for player {player_id}.")
            else:
                logger.debug(f"[remove_player_projections] No cache entry found for player {player_id}.")
        except Exception as e:
            logger.error(f"[remove_player_projections] Error removing full projection node for player {player_id}: {e}")

    def remove_projection(self, player_id: str, projection_id: str, league_abbr: str):
        try:
            key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"

            if key in self.cache:
                data = json.loads(self.cache[key])
                projections = data.get("projections", {})

                if projection_id in projections:
                    del projections[projection_id]

                    if not projections:
                        del self.cache[key]
                        logger.info(f"[remove_projection] No more projections for {player_id}. Key removed from cache.")
                    else:
                        data["projections"] = projections
                        self.cache[key] = json.dumps(data)
                        logger.info(f"[remove_projection] Removed projection {projection_id} from player {player_id}.")
                else:
                    logger.debug(f"[remove_projection] Projection {projection_id} not found for player {player_id}.")
        except Exception as e:
            logger.error(f"[remove_projection] Error removing projection {projection_id} for {player_id}: {e}")

    def set_projection_bulk(self, projections_by_player: Dict[str, Dict[str, Any]], league: str) -> int:
        """
        Bulk cache full player records by player ID.
        Keys will follow format: {platform_abbr}:projections:{league}:{player_id}
        """
        count = 0
        try:
            for player_id, player_data in projections_by_player.items():
                key = f"{self.platform_abbr}:projections:{league}:{player_id}"
                self.cache[key] = json.dumps(player_data)
                count += 1
            logging.info(f"[set_projection_bulk] Cached {count} full player records for {league.upper()} ({self.platform_abbr})")
        except Exception as e:
            logging.error(f"[set_projection_bulk] Failed bulk set: {e}")
        return count
