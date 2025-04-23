import firebase_admin
from firebase_admin import credentials, db
import logging

# Initialize Firebase Admin SDK
def initialize_firebase():
    try:
        cred = credentials.Certificate("./config/serviceAccountKey.json")  # Path to your Firebase service account key
        firebase_admin.initialize_app(cred, {
            "databaseURL": "https://sportbets-1e08a-default-rtdb.firebaseio.com/"  # Replace with your Firebase database URL
        })
        logging.info("Firebase initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing Firebase: {e}")
        raise

# Function to clear specified references
def clear_references(refs_to_clear):
    try:
        for ref in refs_to_clear:
            logging.info(f"Clearing data at reference: {ref}")
            db.reference(ref).delete()
            logging.info(f"Data cleared at reference: {ref}")
    except Exception as e:
        logging.error(f"Error clearing references: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    # Initialize Firebase
    initialize_firebase()

    # References to clear
    references = [
        "prizepicksMLB",
        "prizepicksNFL",
        "prizepicksNHL",
        "prizepicksNBA"
    ]

    # Clear the references
    clear_references(references)

    logging.info("All specified references have been cleared.")