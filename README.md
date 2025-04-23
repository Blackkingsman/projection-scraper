# 🏈 Projection Scraper

A scalable backend engine built for scraping, processing, and syncing real-time player projection data across multiple sports and betting platforms. This system listens to real-time Firebase events, scrapes projections from external APIs, and updates the front-end with clean, merged player + projection data.

---

## 🚀 Features

- 🔄 **Real-time sync** with Firebase using a listener.
- 🧠 **Smart processor** that filters, matches, and updates projection data.
- 📦 **In-memory caching** for fast access (non-persistent, syncs with Firebase/Firestore on startup).
- 🔌 Polls **PrizePicks API** for projection data.
- 🛑 Graceful shutdown via signal handling.
- 📈 Multi-league support (NFL, NBA, NHL, MLB).

---

## 📁 Project Structure

```
projection-scraper/
├── config/
│   ├── .env                         # Environment variables
│   └── serviceAccountKey.json       # Firebase service account credentials
│
├── managers/
│   ├── firebase_manager.py          # Handles reads/writes to Firebase Realtime DB
│   ├── firestore_manager.py         # Listens to Firestore updates
│   ├── cache_manager.py             # In-memory caching for players & projections
│
├── utils/
│   ├── data_fetcher.py              # Handles HTTP requests to fetch projections
│   ├── projection_processor.py      # Filters, maps, and manages projection data
│   ├── realtime_listener.py         # Listens to Firebase updates in real time
│   └── signal_handler.py            # Manages graceful shutdown via CTRL+C
│
├── main.py                          # Primary runtime script – starts everything
├── requirements.txt                 # Python dependencies
├── .gitignore
```

---

## 🧠 How It Works

- `main.py` is the **brain** of the application. It:
  - Initializes all managers and utilities.
  - Starts a listener for Firebase updates.
  - Polls the **PrizePicks API** for projection data.
  - Pushes updated data back to Firebase.
  - Uses **in-memory caching** for fast access (non-persistent, syncs with Firebase/Firestore on startup).

- `firebase_manager.py` abstracts all Firebase operations.
- `firestore_manager.py` listens for Firestore updates and triggers callbacks.
- `cache_manager.py` provides in-memory caching for players and projections.
- `projection_processor.py` handles matching players with projections, removes outdated entries, and ensures accuracy before uploading.
- `realtime_listener.py` listens for live updates and makes real-time adjustments.
- `signal_handler.py` ensures a clean shutdown of all threads and async loops.

---

## ⚙️ Setup Instructions

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Add your Firebase credentials in config/serviceAccountKey.json
# Add environment variables in config/.env
```

---

## 💡 Example Usage

```bash
python main.py
```

### Debug Mode
To enable debug mode for detailed logging:
```bash
python main.py -d
```

### Instructions
- **Enable Debug Mode:** Run the script with the `-d` flag.
- **Disable Debug Mode:** Restart the script without the `-d` flag.
- **Stop the Application:** Press `Ctrl+C` or kill the terminal.

---

