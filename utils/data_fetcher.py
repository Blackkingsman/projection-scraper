import logging
import asyncio
import random
import os
from curl_cffi import requests
import json
from typing import Optional, Dict, Any
class DataFetcher:



    def __init__(self):
        # Session setup for making HTTP requests
        self.session = requests.Session()

        # Fetch the API key from environment variables
        self.XAPI_KEY = os.getenv("PRIZEPICKS_SCRAPER_KEY")

        # Headers template for ProxyFetch API requests
        self.headers = {
            
        'Content-Type': 'application/json',
          'x-api-key': self.XAPI_KEY, 
        }

    async def fetch_projections(self, league_id: int, platform: str):
        """
        Dispatch projection fetching to the appropriate platform handler.
        """
        try:
            if platform == "prizepicks":
                return await self.fetch_prizepicks_projections(league_id)
            elif platform == "underdog":
                return await self.fetch_underdog_projections(league_id)
            else:
                raise ValueError(f"[fetch_projections] Unsupported platform '{platform}'.")
        except Exception as e:
            logging.error(f"[fetch_projections] Error fetching projections for platform '{platform}': {e}")
            return []

    async def fetch_prizepicks_projections(self, league_id: int):
        """
        Fetch projections from the ProxyFetch API for PrizePicks.
        """
        try:
            body = json.dumps({"league_ids": [league_id]})
            logging.info(f"[fetch_prizepicks_projections] Sending request with body: {body}")

            proxyfetch_url = "http://192.168.1.5:8000/fetch-prizepicks"
            response = self.session.post(proxyfetch_url, headers=self.headers, data=body)

            if response.status_code == 200:
                data = response.json().get('projections', [])
                logging.info(f"[fetch_prizepicks_projections] Successfully fetched {len(data[0])} projections.")
                return data[0]
            elif response.status_code == 403:
                logging.warning("[fetch_prizepicks_projections] Access forbidden, retrying...")
                return await self.fetch_prizepicks_projections(league_id)
            else:
                raise RuntimeError(f"[fetch_prizepicks_projections] Failed to fetch: status {response.status_code}")

        except Exception as e:
            logging.error(f"[fetch_prizepicks_projections] Exception occurred: {e}")
            return []

    #We should not be making this request here we need to pass this off to proxy fetcher
    async def fetch_player_data(self, player_id: str) -> Optional[Dict[str, Any]]:
        """Fetch individual player data from the PrizePicks API with a random rate limit (1-3 seconds)."""
        url = f"https://api.prizepicks.com/players/{player_id}"

        try:
            # Rate limit with a random delay between 1 and 3 seconds
            delay = random.uniform(1, 3)
            logging.info(f"Rate limiting: Sleeping for {delay:.2f} seconds before request.")
            await asyncio.sleep(delay)

            # Perform the HTTP GET request to PrizePicks API
            response = self.session.get(url, impersonate="chrome110", headers=self.headers)

            if response.status_code == 200:
                data = response.json().get('data', {}).get('attributes', {})
                if data:
                    logging.info(f"Successfully fetched data for player {player_id}.")
                    return data
                else:
                    logging.warning(f"No player data found for player {player_id}.")
                    return None

            elif response.status_code == 403:
                # Handle 403 response by retrying
                logging.warning(f"Access forbidden for player {player_id}, retrying...")
                return await self.fetch_player_data(player_id)

            else:
                logging.error(f"Failed to fetch player {player_id}, status: {response.status_code}")
                return None

        except Exception as e:
            logging.error(f"[fetch_player_data] Error fetching player {player_id}: {e}")
            return None