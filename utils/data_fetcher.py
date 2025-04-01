import logging
import os
from typing import Optional, Dict, Any
import httpx
from dotenv import load_dotenv
from pathlib import Path

# Correctly resolve the path to the `.env` file
dotenv_path = Path(__file__).resolve().parent.parent / "config" / ".env"
load_dotenv(dotenv_path=dotenv_path)

class DataFetcher:
    def __init__(self, platform_abbr: str):
        self.platform_abbr = platform_abbr

        self.XAPI_KEY = os.getenv('PRIZEPICKS_SCRAPER_KEY')
        if not self.XAPI_KEY:
            raise EnvironmentError("PRIZEPICKS_SCRAPER_KEY environment variable must be set!")

        self.BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.XAPI_KEY,
        }

    async def fetch_projections(self, league_id: int, platform: str):
        try:
            if platform == "prizepicks":
                return await self.fetch_prizepicks_projections(league_id)
            elif platform == "underdog":
                return await self.fetch_underdog_projections(league_id)
            else:
                raise ValueError(f"[fetch_projections] Unsupported platform '{platform}'.")
        except Exception as e:
            logging.error(f"[fetch_projections] Error fetching projections: {e}")
            return []

    async def fetch_prizepicks_projections(self, league_id: int):
        endpoint = f"{self.BASE_URL}/fetch-prizepicks"
        try:
            league_id = int(league_id)
            logging.info(f"[fetch_prizepicks_projections] Requesting league_id: {league_id}")

            # Explicitly set timeout to 10 seconds
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(endpoint, headers=self.headers, json={"league_id": league_id})

            response.raise_for_status()
            data = response.json().get('projections', [])
            if data:
                logging.info(f"[fetch_prizepicks_projections] Retrieved {len(data)} projections.")
            else:
                logging.warning("[fetch_prizepicks_projections] No projections found.")
            return data

        except httpx.HTTPError as e:
            logging.error(f"[fetch_prizepicks_projections] HTTP error: {e}")
            return []
        except Exception as e:
            logging.error(f"[fetch_prizepicks_projections] Exception: {e}")
            return []

    async def fetch_underdog_projections(self, league_id: int):
        endpoint = f"{self.BASE_URL}/fetch-underdog"
        try:
            league_id = int(league_id)
            logging.info(f"[fetch_underdog_projections] Requesting league_id: {league_id}")

            # Explicitly set timeout to 10 seconds
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(endpoint, headers=self.headers, json={"league_id": league_id})

            response.raise_for_status()
            data = response.json().get('projections', [])
            if data:
                logging.info(f"[fetch_underdog_projections] Retrieved {len(data)} projections.")
            else:
                logging.warning("[fetch_underdog_projections] No projections found.")
            return data

        except httpx.HTTPError as e:
            logging.error(f"[fetch_underdog_projections] HTTP error: {e}")
            return []
        except Exception as e:
            logging.error(f"[fetch_underdog_projections] Exception: {e}")
            return []

    async def fetch_player_data(self, player_id: str) -> Optional[Dict[str, Any]]:
        if self.platform_abbr == "pp":
            endpoint = f"{self.BASE_URL}/fetch-prizepicks-player"
        elif self.platform_abbr == "ud":
            endpoint = f"{self.BASE_URL}/fetch-underdog-player"
        else:
            raise ValueError(f"[fetch_player_data] Unsupported platform: {self.platform_abbr}")

        try:
            # Explicitly set timeout to 10 seconds
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(endpoint, headers=self.headers, json={"player_id": player_id})

            response.raise_for_status()
            data = response.json()
            if data:
                logging.info(f"[fetch_player_data] Player data retrieved: {player_id}")
                return data
            else:
                logging.warning(f"[fetch_player_data] No data for player {player_id}")
                return None

        except httpx.HTTPError as e:
            logging.error(f"[fetch_player_data] HTTP error for player {player_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"[fetch_player_data] Exception for player {player_id}: {e}")
            return None
