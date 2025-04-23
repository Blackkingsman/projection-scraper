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
