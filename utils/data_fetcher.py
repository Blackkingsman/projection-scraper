import logging
import requests as requests  # avoid name clash with curl_cffi.requests
from typing import Optional, Dict, Any
import httpx
class DataFetcher:
    def __init__(self, platform_abbr:str):
        # Session setup for making HTTP requests
        self.session = requests.Session()
        self.platform_abbr = platform_abbr
        # Fetch the API key from environment variables
        self.XAPI_KEY = "KOMO8gWTJgyf-N8vRB3lGA-SBirPQhzJLFxDEubIwQA9ZagwCgOHJX8i6NDQ67EaRw0"

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
        Fetch projections from the ProxyFetch API for PrizePicks using httpx (async).
        """
        try:
            league_id = int(league_id)
            logging.info(f"[fetch_prizepicks_projections] Sending request for league_id: {league_id}")

            proxyfetch_url = "http://192.168.1.5:8000/fetch-prizepicks"

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    proxyfetch_url,
                    headers=self.headers,
                    json={"league_id": league_id}
                )

            if response.status_code == 200:
                data = response.json().get('projections', [])
                if isinstance(data, list) and len(data) > 0:
                    logging.info(f"[fetch_prizepicks_projections] Successfully fetched {len(data)} projections.")
                    return data
                else:
                    logging.warning("[fetch_prizepicks_projections] No projections returned.")
                    return []

            elif response.status_code == 403:
                logging.warning("[fetch_prizepicks_projections] Access forbidden, retrying...")
                return await self.fetch_prizepicks_projections(league_id)

            else:
                raise RuntimeError(f"[fetch_prizepicks_projections] Failed to fetch: status {response.status_code}")

        except Exception as e:
            logging.error(f"[fetch_prizepicks_projections] Exception occurred: {e}")
            return []
    async def fetch_player_data(self, player_id: str) -> Optional[Dict[str, Any]]:
        try:
            if self.platform_abbr == "pp":
                url = "http://192.168.1.5:8000/fetch-prizepicks-player"
            elif self.platform_abbr == "ud":
                url = "http://192.168.1.5:8000/fetch-underdog-player"
            else:
                raise ValueError(f"[fetch_player_data] Unsupported platform abbreviation: {self.platform_abbr}")

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=self.headers, json={"player_id": player_id})

            if response.status_code == 200:
                data = response.json()
                if data:
                    logging.info(f"Successfully fetched data for player {player_id} via ProxyFetch ({self.platform_abbr}).")
                    return data
                else:
                    logging.warning(f"No data found for player {player_id}.")
                    return None
            else:
                logging.error(f"ProxyFetch returned {response.status_code} for player {player_id}")
                return None

        except Exception as e:
            logging.error(f"[fetch_player_data] Error calling ProxyFetch for {player_id}: {e}")
            return None
