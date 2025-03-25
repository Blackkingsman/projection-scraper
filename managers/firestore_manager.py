from google.cloud import firestore
import logging

class FirestoreManager:
    def __init__(self, credential_path):
        self.db = firestore.Client.from_service_account_json(credential_path)

    def listen_sports_flags(self, callback):
        collection_ref = self.db.collection('PrizePicksSportsFlag')

        # Define the Firestore snapshot listener
        def on_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                sport_name = change.document.id
                data = change.document.to_dict()
                active = data.get('active', False)
                league_id = data.get('id')

                logging.info(f"Firestore update for {sport_name}: active={active}, id={league_id}")

                # Call dispatcher callback with the relevant details
                callback(sport_name, active, league_id)

        # Start listening
        self.listener = collection_ref.on_snapshot(on_snapshot)
        logging.info("Started Firestore sports flag listener.")

    def stop_listener(self):
        if hasattr(self, 'listener'):
            self.listener.unsubscribe()
            logging.info("Firestore listener stopped.")
