from google.cloud import firestore
import logging
from typing import Callable


class FirestoreManager:
    """
    Manages Firestore interactions, including listening for updates and managing sports flags.
    """

    def __init__(self, credential_path: str):
        """
        Initialize the FirestoreManager.

        Args:
            credential_path (str): Path to the Firestore service account JSON file.
        """
        self.db = firestore.Client.from_service_account_json(credential_path)
        self.listener = None  # Placeholder for the Firestore listener

    def listen_sports_flags(self, callback: Callable[[str, bool, str], None]) -> None:
        """
        Listen for updates to the 'PrizePicksSportsFlag' collection in Firestore.

        Args:
            callback (Callable[[str, bool, str], None]): Function to call when a Firestore update occurs.
                The callback receives the sport name, active status, and league ID.
        """
        collection_ref = self.db.collection('PrizePicksSportsFlag')

        # Fetch the list of sports in the PrizePicksSportsFlag collection
        valid_sports = {doc.id for doc in collection_ref.stream()}

        # Define the Firestore snapshot listener
        def on_snapshot(col_snapshot, changes, read_time):
            """
            Handle Firestore snapshot updates.

            Args:
                col_snapshot: The collection snapshot.
                changes: List of changes in the snapshot.
                read_time: The time the snapshot was read.
            """
            for change in changes:
                sport_name = change.document.id
                if sport_name not in valid_sports:
                    logging.info(f"[FirestoreManager] Ignoring update for irrelevant sport: {sport_name}")
                    continue

                data = change.document.to_dict()
                active = data.get('active', False)
                league_id = data.get('id')

                logging.info(f"[FirestoreManager] Firestore update for {sport_name}: active={active}, id={league_id}")

                # Call dispatcher callback with the relevant details
                callback(sport_name, active, league_id)

        # Start listening to the Firestore collection
        self.listener = collection_ref.on_snapshot(on_snapshot)
        logging.info("[FirestoreManager] Started Firestore sports flag listener.")

    def stop_listener(self) -> None:
        """
        Stop the Firestore listener if it is running.
        """
        if self.listener:
            self.listener.unsubscribe()
            logging.info("[FirestoreManager] Firestore listener stopped.")
        else:
            logging.warning("[FirestoreManager] No active Firestore listener to stop.")

    def update_league_flag(self, league_id: int, active: bool):
        """
        Update the active flag for a league in Firestore.

        Args:
            league_id (int): The ID of the league.
            active (bool): The active status to set.
        """
        try:
            league_ref = self.db.collection("PrizePicksSportsFlag").document(str(league_id))
            league_ref.update({"active": active})
            logging.info(f"[FirestoreManager] Updated league {league_id} active flag to {active}.")
        except Exception as e:
            logging.error(f"[FirestoreManager] Failed to update league {league_id} active flag: {e}")

    def get_league_id(self, sport_name: str) -> str:
        """
        Fetch the league ID for a given sport from Firestore.

        Args:
            sport_name (str): The name of the sport.

        Returns:
            str: The league ID associated with the sport.
        """
        try:
            sport_ref = self.db.collection("PrizePicksSportsFlag").document(sport_name)
            sport_data = sport_ref.get().to_dict()
            if sport_data and "id" in sport_data:
                return sport_data["id"]
            else:
                logging.warning(f"[FirestoreManager] No league ID found for sport: {sport_name}")
                return None
        except Exception as e:
            logging.error(f"[FirestoreManager] Error fetching league ID for sport {sport_name}: {e}")
            return None

    def get_projection_ref(self, league_id: str):
        """
        Fetch the projection reference for a given league ID from Firestore.

        Args:
            league_id (str): The ID of the league.

        Returns:
            Firestore Document Reference: The reference to the projection document.
        """
        try:
            return self.db.collection("Projections").document(league_id)
        except Exception as e:
            logging.error(f"[FirestoreManager] Error fetching projection reference for league ID {league_id}: {e}")
            return None
