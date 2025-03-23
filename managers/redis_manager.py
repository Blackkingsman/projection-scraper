import redis
import os
import json
from urllib.parse import urlparse
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv
load_dotenv("./config/.env")


logger = logging.getLogger("RedisManager")

class RedisManager:
    def __init__(self):
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            raise ValueError("REDIS URL enviornment variable not set.")
        
        redis_url = urlparse(redis_url)
        
        #Ensure port is valid, default to 6379 if not specified
        
        port = redis_url.port if redis_url.port else 6379
    
        try:
            # Assign to self.client for access throughout the class
            self.client = redis.Redis(
                host=redis_url.hostname,
                port=port,
                password=redis_url.password,
                ssl=(redis_url.scheme == "rediss"),
                ssl_cert_reqs=None
            )
            logger.info("Successfully connected to Redis")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
          
    # Player Cache Functions
    def get_player(self, player_id: str) -> Optional[Dict[str, Any]]:
        try:
            data = self.client.get(player_id)
            if data:
                return json.loads(data) 
            else:
                logger.info(f"No Data found for player_id: {player_id}")
                return None
        except (json.JSONDecodeError,redis.RedisError) as e:
            logger.error(f"Error retrieving player data for player_id {player_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unkonwn Execption caught please Review get_player Function: {e}")
            return None
        
    def set_player(self, player_id: str, player_data: Dict[str, Any]) -> bool:
        try:
            # Serialize player_data dictionary into JSON before storing
            serialized_data = json.dumps(player_data)
            # Set data in Redis
            self.client.set(player_id, serialized_data)
            logger.info(f"Player data set in Redis for player_id: {player_id}")
            return True
        except (redis.RedisError, TypeError, json.JSONDecodeError) as e:
            # Catch serialization and Redis connection errors
            logger.error(f"Failed to set player data in Redis for player_id {player_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unkonwn Execption caught please Review set_player Function: {e}")
            return False
        
    def remove_player(self, player_id: str):
        """
        Remove a player's data from the Redis cache.
        """
        try:
            deleted = self.client.delete(f"player:{player_id}")
            if deleted:
                logger.info(f"Removed player {player_id} from Redis cache.")
            else:
                logger.info(f"No player found in Redis for player_id: {player_id}.")
        except redis.RedisError as e:
            logger.error(f"[remove_player] Redis error while deleting player {player_id}: {e}")
        except Exception as e:
            logger.error(f"[remove_player] Unexpected error while deleting player {player_id}: {e}")

    def player_exists(self, player_id: str) -> bool:
        #checking to see if the player exists return true if it doesnt return false if it does 
        try:
            return self.client.exists(player_id) == 1
        except Exception as e:
            #catching any potential errors here
            logger.error(f"Error checking player existence in Redis:{e}")
            return False
        
    # --- Projection Cache Operations ---

    def get_projection(self, player_id: str) -> dict:
        """
        Retrieve projections for a single player from Redis.
        """
        try:
            key = f"projections:{player_id}"
            projection_data = self.client.get(key)

            if projection_data:
                return json.loads(projection_data)
            else:
                return {}

        except redis.RedisError as redis_err:
            logger.error(f"Redis error in get_projection for player {player_id}: {redis_err}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in get_projection for player {player_id}: {e}")
            return {}
        
    def get_all_projections(self) -> Dict[str, dict]:
        projections = {}

        try:
            for key in self.client.scan_iter("projections:*"):
                player_id = key.decode().split(":")[1]
                projection_data = self.client.get(key)
                if projection_data:
                    projections[player_id] = json.loads(projection_data)

        except redis.RedisError as redis_err:
            logger.error(f"[RedisManager.get_all_projections] Redis error: {redis_err}")
        except Exception as e:
            logger.error(f"[RedisManager.get_all_projections] Unexpected error: {e}")

        return projections
    
    def update_projection_cache(self, player_id: str, projection_data: dict):
        """
        Update the projection cache with new or updated data for the given player.
        """
        try:
            key = f"projections:{player_id}"

            # Get existing projections if any
            existing_data = self.client.get(key)
            if existing_data:
                existing_projections = json.loads(existing_data)
            else:
                existing_projections = {}

            # Merge/update projections
            for proj_id, proj_details in projection_data.items():
                existing_projections[proj_id] = proj_details

            self.client.set(key, json.dumps(existing_projections))
            logger.info(f"Updated projection cache for player {player_id}.")
        except redis.RedisError as e:
            logger.error(f"Redis error in update_projection_cache: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in update_projection_cache: {e}")


    def remove_projection(self, player_id: str, projection_id: str):
        try:
            key = f"projections:{player_id}"
            projection_data = self.client.get(key)

            if projection_data:
                projection_data = json.loads(projection_data)
                projections = projection_data.get("projections", {})

                if projection_id in projections:
                    del projections[projection_id]
                    logging.info(f"Removed projection {projection_id} for player {player_id} from cache.")

                if projections:
                    # Update the Redis entry with the remaining projections
                    projection_data["projections"] = projections
                    self.client.set(key, json.dumps(projection_data))
                else:
                    # No projections left, remove the whole key
                    self.client.delete(key)
                    logging.info(f"Removed player {player_id} from projection cache after all projections deleted.")

        except redis.RedisError as e:
            logger.error(f"Redis error in remove_projection: {e}")
        except Exception as e:
            logger.error(f"General error in remove_projection: {e}")


    # --- Cache Utility Operations ---

    def count_projections(self) -> int:
        """
        Count the total number of projections stored in Redis.
        """
        try:
            total_projections = 0
            for key in self.client.scan_iter("projections:*"):
                data = self.client.get(key)
                if data:
                    projection_dict = json.loads(data)
                    total_projections += len(projection_dict)
            return total_projections
        except redis.RedisError as e:
            logger.error(f"[count_projections] Redis error: {e}")
            return 0
        except Exception as e:
            logger.error(f"[count_projections] Unexpected error: {e}")
            return 0