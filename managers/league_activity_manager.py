import logging
import httpx
import os
from google.cloud import firestore
from dotenv import load_dotenv
import asyncio
import random

# Load environment variables
load_dotenv(dotenv_path="./config/.env")

class LeagueActivityManager:
    def __init__(self, base_url: str, firestore_manager):
        self.base_url = base_url
        self.firestore_manager = firestore_manager
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': os.getenv('PRIZEPICKS_SCRAPER_KEY')
        }

    async def fetch_leagues(self):
        """
        Fetch leagues from the custom API.

        Returns:
            list: List of leagues with their projection counts.
        """
        endpoint = "https://proxy-fetch.duckdns.org/fetch-leagues"
        logging.info(f"[LeagueActivityManager] Preparing to fetch leagues from hardcoded endpoint: {endpoint}")
        logging.debug(f"[LeagueActivityManager] Headers: {self.headers}")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(endpoint, headers=self.headers)
                logging.info(f"[LeagueActivityManager] HTTP Request: POST {endpoint} - Status Code: {response.status_code}")
                logging.debug(f"[LeagueActivityManager] Response Content: {response.text}")
            response.raise_for_status()
            logging.info(f"[LeagueActivityManager] Successfully fetched leagues from API.")
            leagues = response.json().get("leagues", {}).get("data", [])

            # Fetch the list of relevant sports from Firestore
            valid_sports = {doc.id for doc in self.firestore_manager.db.collection('PrizePicksSportsFlag').stream()}

            # Filter leagues based on relevant sports
            filtered_leagues = [
                league for league in leagues
                if league.get("attributes", {}).get("name") in valid_sports
            ]

            logging.info(f"[LeagueActivityManager] Filtered leagues: {filtered_leagues}")

            # Further filter based on Firestore 'active' flag
            active_leagues = []
            for league in filtered_leagues:
                league_id = int(league.get("id"))
                # Read Firestore flag document
                doc_ref = self.firestore_manager.db.collection('PrizePicksSportsFlag').document(str(league_id))
                doc = doc_ref.get()
                is_active = doc.to_dict().get('active', True) if doc.exists else True
                if is_active:
                    active_leagues.append(league)
            logging.info(f"[LeagueActivityManager] Active leagues after Firestore flag filter: {active_leagues}")
            return active_leagues
        except httpx.HTTPError as e:
            logging.error(f"[LeagueActivityManager] HTTP error during fetch-leagues: {e}")
            logging.debug(f"[LeagueActivityManager] Possible network issue or invalid endpoint: {endpoint}")
            return []
        except Exception as e:
            logging.error(f"[LeagueActivityManager] General error during fetch-leagues: {e}")
            logging.debug(f"[LeagueActivityManager] Check hardcoded endpoint and network connectivity: {endpoint}")
            return []

    async def update_league_activity(self, processed_league_ids):
        """
        Update league activity flags in Firestore based on projection counts.

        Args:
            processed_league_ids (list): List of league IDs currently being processed.
        """
        leagues = await self.fetch_leagues()
        league_map = {int(league["id"]): league for league in leagues}

        for league_id, league_data in league_map.items():
            projections_count = league_data.get("attributes", {}).get("projections_count", 0)
            should_be_active = projections_count > 0

            # Skip flag updates for leagues not currently being processed
            if league_id not in processed_league_ids:
                logging.info(f"[LeagueActivityManager] Skipping flag update for league ID {league_id} as it is not currently being processed.")
                continue

            try:
                league_name = league_data.get("attributes", {}).get("name", "Unknown League")
                logging.info(f"[LeagueActivityManager] League '{league_name}' (ID: {league_id}) projections_count = {projections_count}")

                # Ensure flag update only happens if historicals are processed
                if projections_count == 0:
                    logging.info(f"[LeagueActivityManager] Skipping flag update for '{league_name}' (ID: {league_id}) as historicals are being processed.")
                    continue

                # Update Firestore flag
                self.firestore_manager.update_league_flag(league_id, active=should_be_active)
                logging.info(f"[LeagueActivityManager] Updated league '{league_name}' (ID: {league_id}) active flag to {should_be_active}")

            except Exception as e:
                logging.error(f"[LeagueActivityManager] Failed to update league '{league_id}' activity: {e}")

    async def probe_leagues_watcher(self, active_sports):
        """
        Probe the leagues watcher every 30-45 seconds to update league flags.

        Args:
            active_sports (list): List of sports currently being processed.
        """
        while True:
            try:
                leagues = await self.fetch_leagues()
                league_map = {int(league["id"]): league for league in leagues}

                for league_id, league_data in league_map.items():
                    projections_count = league_data.get("attributes", {}).get("projections_count", 0)
                    should_be_active = projections_count > 0

                    league_name = league_data.get("attributes", {}).get("name", "Unknown League")

                    # Always turn flags on unless specific conditions to turn them off are met
                    if league_name in active_sports:
                        logging.info(f"[LeagueActivityManager] Blocking flag update for '{league_name}' (ID: {league_id}) as it is currently being processed.")
                        continue

                    try:
                        logging.info(f"[LeagueActivityManager] Probing league '{league_name}' (ID: {league_id}) projections_count = {projections_count}")

                        # Update Firestore flag
                        self.firestore_manager.update_league_flag(league_id, active=should_be_active)
                        logging.info(f"[LeagueActivityManager] Updated league '{league_name}' (ID: {league_id}) active flag to {should_be_active}")

                    except Exception as e:
                        logging.error(f"[LeagueActivityManager] Failed to update league '{league_id}' activity during probe: {e}")

                await asyncio.sleep(random.randint(30, 45))  # Sleep for 30-45 seconds

            except Exception as e:
                logging.error(f"[LeagueActivityManager] Error during leagues watcher probe: {e}")

    async def get_projections_count_for_league(self, league_id):
        """
        Fetch the current projections count for a single league.

        Args:
            league_id (int or str): League ID to check.

        Returns:
            int: The projections_count for the league or 0 if not found.
        """
        try:
            leagues = await self.fetch_leagues()
            for league in leagues:
                if int(league.get("id", -1)) == int(league_id):
                    return league.get("attributes", {}).get("projections_count", 0)
        except Exception as e:
            logging.error(f"[LeagueActivityManager] Error fetching projections count for league {league_id}: {e}")
        return 0
