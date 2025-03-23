import logging
import asyncio
import random
import os
from curl_cffi import requests
import json
from typing import List
class DataFetcher:

    # Global lock to ensure only one fetch request handles the VPN switch at a time
    vpn_lock = asyncio.Lock()

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

    async def fetch_projections(self, league_ids):
        """Fetch projections from the ProxyFetch API, sending league_ids in the body."""
        try:
            # Correctly structure the body
            body = json.dumps({"league_ids": [league_ids]})
            logging.info(f"Sending request with body: {body}")

            # Send the request to ProxyFetch API with the league_ids in the body
            proxyfetch_url = "http://192.168.1.5:8000/fetch-prizepicks"
            response = self.session.post(proxyfetch_url, headers=self.headers, data=body)

            if response.status_code == 200:
                data = response.json().get('projections', [])
               
                if data:
                    logging.info(f"Successfully fetched {len(data[0])} projections.")
                    return data[0]
                else:
                    logging.warning("No projections found in the response.")
                    return None

            elif response.status_code == 403:
                logging.warning("Access forbidden, retrying...")
                return await self.fetch_projections(league_ids)

            else:
                logging.error(f"Failed to fetch projections, status: {response.status_code}")
                return None

        except Exception as e:
            logging.error(f"Exception occurred while fetching projections from ProxyFetch: {e}")
            return None

    #We should not be making this request here we need to pass this off to proxy fetcher
    async def fetch_player_data(self, player_id):
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
            logging.error(f"Error fetching player {player_id}: {e}")
            return None

