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
    def set_ready(self):
        """
        Mark the LeagueActivityManager as ready (initial Firestore state loaded).
        """
        if not hasattr(self, 'ready_event'):
            import threading
            self.ready_event = threading.Event()
        self.ready_event.set()

    def wait_until_ready(self):
        """
        Block until the LeagueActivityManager is ready (initial Firestore state loaded).
        """
        if not hasattr(self, 'ready_event'):
            import threading
            self.ready_event = threading.Event()
        self.ready_event.wait()
    def update_league_flag_state(self, league_id, active):
        """
        Update the internal league_flag_status dict for a given league.
        This should be called by the main dispatcher callback in main.py whenever a flag changes.
        """
        if not hasattr(self, 'league_flag_status'):
            self.league_flag_status = {}
        self.league_flag_status[str(league_id)] = active
    def __init__(self, base_url: str, firestore_manager, watched_league_ids=None):
        self.base_url = base_url
        self.firestore_manager = firestore_manager
        # limit auto-probe to only these leagues if provided
        # Always probe all configured leagues for auto-probe
        self.watched_league_ids = set(watched_league_ids) if watched_league_ids is not None else set()
        self.auto_probe_league_ids = set(watched_league_ids) if watched_league_ids is not None else set()
        # Event to signal the auto-probe can start (initial Firestore flags loaded)
        import threading
        self.ready_event = threading.Event()
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': os.getenv('PRIZEPICKS_SCRAPER_KEY'),
            "CF-Access-Client-Id": os.getenv("CF_ACCESS_CLIENT_ID"),
            "CF-Access-Client-Secret": os.getenv("CF_ACCESS_CLIENT_SECRET")
        }

    async def fetch_leagues(self):
        """
        Fetch leagues from the custom API.

        Returns:
            list: List of leagues with their projection counts.
        """
        endpoint = "https://proxyfetch.philpicks.ai/fetch-leagues"
        logging.info(f"[LeagueActivityManager] Preparing to fetch leagues from hardcoded endpoint: {endpoint}")
        logging.debug(f"[LeagueActivityManager] Headers: {self.headers}")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(endpoint, headers=self.headers)
                logging.info(f"[LeagueActivityManager] HTTP Request: POST {endpoint} - Status Code: {response.status_code}")
                logging.debug(f"[LeagueActivityManager] Response Content: {response.text}")
                        
            response.raise_for_status()
            logging.info(f"[LeagueActivityManager] Successfully fetched leagues from API.")
            
            response_json = response.json()
            logging.info(f"[LeagueActivityManager] Response JSON keys: {list(response_json.keys()) if response_json else 'None'}")
            
            # Check different possible structures
            leagues = None
            if "leagues" in response_json and "data" in response_json["leagues"]:
                leagues = response_json["leagues"]["data"]
                logging.info(f"[LeagueActivityManager] Found leagues under 'leagues.data': {len(leagues)} items")
            elif "data" in response_json:
                leagues = response_json["data"]
                logging.info(f"[LeagueActivityManager] Found leagues under 'data': {len(leagues)} items")
            else:
                logging.warning(f"[LeagueActivityManager] Could not find leagues in expected keys. Available keys: {list(response_json.keys())}")
                leagues = []

            # Fetch the list of relevant sports from Firestore
            valid_sports = {doc.id for doc in self.firestore_manager.db.collection('PrizePicksSportsFlag').stream()}

            # Filter leagues based on relevant sports
            filtered_leagues = [
                league for league in leagues
                if league.get("attributes", {}).get("name") in valid_sports
            ]

        # logging.info(f"[LeagueActivityManager] Filtered leagues: {filtered_leagues}")

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
            # logging.info(f"[LeagueActivityManager] Active leagues after Firestore flag filter: {active_leagues}")
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
                # restrict to watched leagues if specified
                # For auto-probe, always use all configured league IDs
                if self.auto_probe_league_ids:
                    leagues = [league for league in leagues if int(league.get("id", -1)) in self.auto_probe_league_ids]
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
    
    async def start_auto_league_probe(self):
        """
        Periodically probe all leagues every 2-3 minutes to update Firestore flags (only false→true). Handles log rotation and cleanup.
        """
        import datetime
        import os
        # Use logs/league_watcher/ relative to project root
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        LOG_DIR = os.path.join(PROJECT_ROOT, 'logs', 'league_watcher')
        os.makedirs(LOG_DIR, exist_ok=True)
        current_date = datetime.datetime.now().date()
        log_file = os.path.join(LOG_DIR, f"league_watcher_auto_{current_date}.log")
        auto_logger = logging.getLogger('league_watcher_auto')
        # Remove all handlers first to avoid duplicate logs
        for h in list(auto_logger.handlers):
            auto_logger.removeHandler(h)
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        auto_logger.addHandler(fh)
        auto_logger.setLevel(logging.INFO)
        auto_logger.propagate = False

        def rotate_logs_if_needed():
            nonlocal current_date, log_file, fh
            now = datetime.datetime.now()
            if now.date() != current_date:
                auto_logger.info(f"Hit midnight, rotating log file to new day: {now.date()}")
                # Remove old handler
                auto_logger.removeHandler(fh)
                fh.close()
                # New file
                current_date = now.date()
                log_file = os.path.join(LOG_DIR, f"league_watcher_auto_{current_date}.log")
                fh_new = logging.FileHandler(log_file, encoding='utf-8')
                fh_new.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                auto_logger.addHandler(fh_new)
                fh = fh_new
                auto_logger.info(f"Log file rotated to {log_file}")
            # Clean up old logs (older than 7 days)
            cutoff = now - datetime.timedelta(days=7)
            for fname in os.listdir(LOG_DIR):
                if fname.startswith('league_watcher_auto_') and fname.endswith('.log'):
                    try:
                        date_str = fname[len('league_watcher_auto_'):-4]
                        file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                        if file_date < cutoff.date():
                            os.remove(os.path.join(LOG_DIR, fname))
                            auto_logger.info(f"Deleted old log file: {fname}")
                    except Exception as e:
                        auto_logger.warning(f"Failed to parse or delete log file {fname}: {e}")

        auto_logger.info("Started auto league probe thread.")
        # --- Wait until initial Firestore state is loaded before starting auto-probe loop ---
        if not hasattr(self, 'ready_event'):
            import threading
            self.ready_event = threading.Event()
        auto_logger.info("Waiting for initial Firestore state before starting auto-probe loop...")
        self.ready_event.wait()
        auto_logger.info("Initial Firestore state loaded. Auto-probe loop starting.")
        while True:
            auto_logger.info("Auto-probe cycle: starting probe")
            try:
                rotate_logs_if_needed()
                auto_logger.info("Auto-probe cycle: fetching leagues from API...")
                leagues = await self.fetch_leagues()
                auto_logger.info(f"Auto-probe cycle: fetched {len(leagues)} leagues")
                # restrict to watched leagues if specified
                if self.watched_league_ids is not None:
                    leagues = [league for league in leagues if int(league.get("id", -1)) in self.watched_league_ids]
                for league in leagues:
                    league_id = str(league.get("id", 0))
                    count = league.get("attributes", {}).get("projections_count", 0)
                    active = count > 0
                    # Use internal league_flag_status if available
                    existing_active = self.league_flag_status.get(league_id) if hasattr(self, 'league_flag_status') else None
                    # Only activate if not already active (avoid duplicate True)
                    if active and existing_active is not True:
                        self.firestore_manager.update_league_flag(league_id, True)
                        auto_logger.info(f"Auto-probe activated league ID {league_id} (was {existing_active})")
                    else:
                        auto_logger.debug(f"Auto-probe skipped league ID {league_id} (active={existing_active})")
            except Exception as e:
                auto_logger.error(f"Error in auto league probe: {e}")
            # Sleep for random 2-3 minutes
            await asyncio.sleep(random.randint(120, 180))

    def start_flag_listener(self):
        """
        Start a Firestore listener for PrizePicksSportsFlag and update internal league_flag_status dict.
        """
        self.league_flag_status = {}
        def _flag_callback(sport_name, active, league_id):
            if league_id is not None:
                self.league_flag_status[str(league_id)] = active
        self.firestore_manager.listen_sports_flags(_flag_callback)
