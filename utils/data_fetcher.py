import logging
import os
from typing import Optional, Dict, Any, List
import httpx
from dotenv import load_dotenv
from pathlib import Path

# Correctly resolve the path to the `.env` file
dotenv_path = Path(__file__).resolve().parent.parent / "config" / ".env"
load_dotenv(dotenv_path=dotenv_path)


class DataFetcher:
    """
    Handles fetching data from external APIs for projections, players, and games.
    """

    def __init__(self, platform_abbr: str):
        """
        Initialize the DataFetcher.

        Args:
            platform_abbr (str): Abbreviation for the platform (e.g., "pp" for PrizePicks).
        """
        self.platform_abbr = platform_abbr

        # Load API key from environment variables
        self.XAPI_KEY = os.getenv('PRIZEPICKS_SCRAPER_KEY')
        if not self.XAPI_KEY:
            raise EnvironmentError("PRIZEPICKS_SCRAPER_KEY environment variable must be set!")

        # Load base URL from environment variables or use a default
        self.BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

        # Set default headers for API requests
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.XAPI_KEY,
        }

    async def fetch_projections(self, league_id: int, platform: str) -> List[Dict[str, Any]]:
        """
        Fetch projections for a specific league and platform.

        Args:
            league_id (int): League ID for the projections.
            platform (str): Platform name (e.g., "prizepicks", "underdog").

        Returns:
            List[Dict[str, Any]]: List of projections.
        """
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

    async def fetch_prizepicks_projections(self, league_id: int) -> List[Dict[str, Any]]:
        """
        Fetch projections from the PrizePicks API.

        Args:
            league_id (int): League ID for the projections.

        Returns:
            List[Dict[str, Any]]: List of projections.
        """
        endpoint = f"{self.BASE_URL}/fetch-prizepicks"
        try:
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

    async def fetch_underdog_projections(self, league_id: int) -> List[Dict[str, Any]]:
        """
        Fetch projections from the Underdog API.

        Args:
            league_id (int): League ID for the projections.

        Returns:
            List[Dict[str, Any]]: List of projections.
        """
        endpoint = f"{self.BASE_URL}/fetch-underdog"
        try:
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
        """
        Fetch metadata for a specific player.

        Args:
            player_id (str): Player ID to fetch data for.

        Returns:
            Optional[Dict[str, Any]]: Player metadata if found, otherwise None.
        """
        if self.platform_abbr == "pp":
            endpoint = f"{self.BASE_URL}/fetch-prizepicks-player"
        elif self.platform_abbr == "ud":
            endpoint = f"{self.BASE_URL}/fetch-underdog-player"
        else:
            raise ValueError(f"[fetch_player_data] Unsupported platform: {self.platform_abbr}")

        try:
            logging.info(f"[fetch_player_data] Fetching data for player {player_id}")

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

    async def fetch_game_info(self, game_ids: List[str], batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Fetch raw game information for a list of game IDs in batches.

        Args:
            game_ids (List[str]): List of game IDs to fetch.
            batch_size (int): Number of game IDs to fetch per batch.

        Returns:
            List[Dict[str, Any]]: List of game details.
        """
        endpoint = f"{self.BASE_URL}/games"
        all_game_details = []

        try:
            num_batches = (len(game_ids) + batch_size - 1) // batch_size  # Calculate number of batches
            for i in range(num_batches):
                batch = game_ids[i * batch_size:(i + 1) * batch_size]
                logging.info(f"[fetch_game_info] Fetching batch {i + 1}/{num_batches} with {len(batch)} game IDs.")

                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(
                        endpoint,
                        headers=self.headers,
                        params={"external_game_ids": ",".join(batch), "limit": batch_size}
                    )
                response.raise_for_status()
                batch_data = response.json().get('data', [])
                all_game_details.extend(batch_data)

            logging.info(f"[fetch_game_info] Retrieved details for {len(all_game_details)} games.")
            return all_game_details

        except httpx.HTTPError as e:
            logging.error(f"[fetch_game_info] HTTP error: {e}")
            return []
        except Exception as e:
            logging.error(f"[fetch_game_info] Unexpected error: {e}")
            return []
