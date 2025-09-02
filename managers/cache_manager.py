import json
import logging
import threading
from typing import Optional, Dict, Any
from cachetools import TTLCache

logger = logging.getLogger("CacheManager")


class CacheManager:
    """
    Manages in-memory caching for player metadata and projections using cachetools.
    """

    def __init__(self, platform_abbr: str, maxsize: int = 500_000, ttl: int = 86400):
        """
        Initialize the CacheManager.

        Args:
            platform_abbr (str): Abbreviation for the platform (e.g., "pp" for PrizePicks).
            maxsize (int): Maximum number of items in the cache.
            ttl (int): Time-to-live for cache entries in seconds.
        """
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self.platform_abbr = platform_abbr.lower()
        self._lock = threading.RLock()  # Add reentrant lock for thread safety
        logger.info(f"Initialized in-memory CacheManager for {self.platform_abbr} using cachetools")

    # --- General Cache Operations ---

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value from the cache by key.

        Args:
            key (str): Cache key.

        Returns:
            Optional[str]: Cached value if found, otherwise None.
        """
        try:
            return self.cache.get(key)
        except Exception as e:
            logger.error(f"[get] Error retrieving key '{key}': {e}")
            return None

    # --- Player Metadata Caching ---

    def get_player(self, player_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve player metadata from the cache.

        Args:
            player_id (str): Player ID.

        Returns:
            Optional[Dict[str, Any]]: Player metadata if found, otherwise None.
        """
        try:
            key = f"{self.platform_abbr}:player:{player_id}"
            data = self.cache.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"[get_player] Error for {player_id}: {e}")
            return None

    def set_player(self, player_id: str, player_data: Dict[str, Any]) -> bool:
        """
        Store player metadata in the cache.

        Args:
            player_id (str): Player ID.
            player_data (Dict[str, Any]): Player metadata.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            key = f"{self.platform_abbr}:player:{player_id}"
            self.cache[key] = json.dumps(player_data)
            return True
        except Exception as e:
            logger.error(f"[set_player] Error setting player {player_id}: {e}")
            return False

    def remove_player(self, player_id: str) -> None:
        """
        Remove player metadata from the cache.

        Args:
            player_id (str): Player ID.
        """
        key = f"{self.platform_abbr}:player:{player_id}"
        if key in self.cache:
            del self.cache[key]
            logger.info(f"Removed player {player_id} from cache.")

    def player_exists(self, player_id: str) -> bool:
        """
        Check if a player exists in the cache.

        Args:
            player_id (str): Player ID.

        Returns:
            bool: True if the player exists, False otherwise.
        """
        key = f"{self.platform_abbr}:player:{player_id}"
        return key in self.cache

    def bulk_set_players(self, players: Dict[str, Dict[str, Any]]) -> int:
        """
        Bulk store player metadata in the cache.

        Args:
            players (Dict[str, Dict[str, Any]]): Dictionary of player metadata.

        Returns:
            int: Number of players successfully cached.
        """
        count = 0
        for player_id, player_data in players.items():
            try:
                self.cache[f"{self.platform_abbr}:player:{player_id}"] = json.dumps(player_data)
                count += 1
            except Exception as e:
                logger.error(f"[bulk_set_players] Failed to cache player {player_id}: {e}")
        return count

    def iter_player_keys(self):
        """
        Iterate over all player keys in the cache.

        Yields:
            str: Player cache key.
        """
        prefix = f"{self.platform_abbr}:player:"
        for key in self.cache.keys():
            if key.startswith(prefix):
                yield key

    # --- Projections Caching (by platform + league) ---

    def set_projection(self, player_id: str, player_data: Dict[str, Any], league_abbr: str) -> bool:
        """
        Store player projections in the cache.

        Args:
            player_id (str): Player ID.
            player_data (Dict[str, Any]): Player data including projections.
            league_abbr (str): League abbreviation.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            with self._lock:
                key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"
                self.cache[key] = json.dumps(player_data)
                return True
        except Exception as e:
            logger.error(f"[set_projection] Error for {player_id}: {e}")
            return False

    def get_projection_by_league(self, player_id: str, league_abbr: str) -> Dict[str, Any]:
        """
        Retrieve player projections for a specific league.

        Args:
            player_id (str): Player ID.
            league_abbr (str): League abbreviation.

        Returns:
            Dict[str, Any]: Player projections if found, otherwise an empty dictionary.
        """
        try:
            with self._lock:
                key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"
                raw = self.cache.get(key)
                return json.loads(raw) if raw else {}
        except Exception as e:
            logger.error(f"[get_projection_by_league] Error for {player_id}: {e}")
            return {}

    def clear_projections_for_league(self, league: str) -> int:
        """
        Clear all cached projections for a specific league.

        Args:
            league (str): League abbreviation.

        Returns:
            int: Number of entries removed.
        """
        prefix = f"{self.platform_abbr}:projections:{league}:"
        keys_to_delete = [key for key in self.cache if key.startswith(prefix)]
        for key in keys_to_delete:
            del self.cache[key]
        return len(keys_to_delete)

    def get_all_player_projections_by_league(self, league_abbr: str) -> Dict[str, Dict[str, Any]]:
        """
        Retrieve all cached player projection data for a specific league.

        Args:
            league_abbr (str): League abbreviation.

        Returns:
            Dict[str, Dict[str, Any]]: Cached player projection data.
        """
        result = {}
        prefix = f"{self.platform_abbr}:projections:{league_abbr.upper()}:"

        try:
            with self._lock:
                # Get a snapshot of cache keys to avoid modification during iteration
                cache_keys = list(self.cache.keys())
                
            for key in cache_keys:
                if key.startswith(prefix):
                    try:
                        player_id = key.split(":")[-1]
                        with self._lock:
                            raw = self.cache.get(key)
                        if raw:  # Check if key still exists
                            result[player_id] = json.loads(raw)
                    except Exception as inner_e:
                        logger.warning(f"[get_all_player_projections_by_league] Failed to parse key '{key}': {inner_e}")
        except Exception as e:
            logger.error(f"[get_all_player_projections_by_league] Error during scan: {e}")

        logger.info(f"[get_all_player_projections_by_league] Retrieved {len(result)} players from cache for {league_abbr.upper()}")
        return result

    def remove_player_projections(self, player_id: str, league_abbr: str) -> None:
        """
        Remove all projections for a specific player in a league.

        Args:
            player_id (str): Player ID.
            league_abbr (str): League abbreviation.
        """
        try:
            key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"
            if key in self.cache:
                del self.cache[key]
                logger.info(f"[remove_player_projections] Entire projection node removed for player {player_id}.")
            else:
                logger.debug(f"[remove_player_projections] No cache entry found for player {player_id}.")
        except Exception as e:
            logger.error(f"[remove_player_projections] Error removing full projection node for player {player_id}: {e}")

    def remove_projection(self, player_id: str, projection_id: str, league_abbr: str) -> None:
        """
        Remove a specific projection for a player in a league.

        Args:
            player_id (str): Player ID.
            projection_id (str): Projection ID.
            league_abbr (str): League abbreviation.
        """
        try:
            with self._lock:
                key = f"{self.platform_abbr}:projections:{league_abbr}:{player_id}"

                if key in self.cache:
                    data = json.loads(self.cache[key])
                    projections = data.get("projections", {})

                    # Debug A.J. Brown cache removals
                    if player_id == "206304":
                        logger.warning(f"[AJ_BROWN_DEBUG] Cache removal - Before: {len(projections)} projections, Removing: {projection_id}")

                    if projection_id in projections:
                        del projections[projection_id]
                        
                        # Debug A.J. Brown cache removals
                        if player_id == "206304":
                            logger.warning(f"[AJ_BROWN_DEBUG] Cache removal - After: {len(projections)} projections remaining")

                        if not projections:
                            del self.cache[key]
                            logger.info(f"[remove_projection] No more projections for {player_id}. Key removed from cache.")
                            
                            # Debug A.J. Brown cache removals
                            if player_id == "206304":
                                logger.error(f"[AJ_BROWN_DEBUG] CRITICAL: A.J. Brown cache completely cleared!")
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
        Bulk cache player projections by player ID.

        Args:
            projections_by_player (Dict[str, Dict[str, Any]]): Dictionary of player projections.
            league (str): League abbreviation.

        Returns:
            int: Number of projections successfully cached.
        """
        count = 0
        try:
            for player_id, player_data in projections_by_player.items():
                key = f"{self.platform_abbr}:projections:{league}:{player_id}"
                
                # Debug A.J. Brown cache updates
                if player_id == "206304":
                    proj_count = len(player_data.get("projections", {}))
                    logger.warning(f"[AJ_BROWN_DEBUG] Bulk cache update - Setting {proj_count} projections for A.J. Brown in {league}")
                
                self.cache[key] = json.dumps(player_data)
                count += 1
            logging.info(f"[set_projection_bulk] Cached {count} full player records for {league.upper()} ({self.platform_abbr})")
        except Exception as e:
            logger.error(f"[set_projection_bulk] Failed bulk set: {e}")
        return count
