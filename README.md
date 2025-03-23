
# 🏈 Projection Scraper

A scalable backend engine built for scraping, processing, and syncing real-time player projection data across multiple sports and betting platforms. This system listens to real-time Firebase events, scrapes projections from external APIs, and updates the front-end with clean, merged player + projection data.

---

## 🚀 Features

- 🔄 **Real-time sync** with Firebase using a listener.
- 📦 **Redis-powered caching** for fast access and minimal redundancy.
- 🧠 **Smart processor** that filters, matches, and updates projection data.
- 🔌 Pluggable with **proxy-fetcher** APIs and **Firebase** backends.
- 🛑 Graceful shutdown via signal handling.
- 📈 Ready for multi-sport (NFL, NBA, NHL, etc) scaling.

---

## 📁 Project Structure

```
projection-scraper/
├── config/
│   ├── .env                         # Environment variables
│   └── serviceAccountKey.json      # Firebase service account credentials
│
├── managers/
│   ├── firebase_manager.py         # Handles reads/writes to Firebase Realtime DB
│   ├── redis_manager.py            # Caching layer for players & projections
│   └── cache_manager_old.py        # Legacy in-memory cache (now replaced)
│
├── utils/
│   ├── data_fetcher.py             # Handles HTTP requests to fetch projections
│   ├── projection_processor.py     # Filters, maps, and manages projection data
│   ├── realtime_listener.py        # Listens to Firebase updates in real time
│   └── signal_handler.py           # Manages graceful shutdown via CTRL+C
│
├── main.py                         # Primary runtime script – starts everything
├── requirements.txt                # Python dependencies
├── .gitignore
```

---

## 🧠 How It Works

- `main.py` is the **brain** of the application. It:
  - Initializes all managers and utilities.
  - Starts a listener for Firebase updates.
  - Fetches projection data from APIs and filters it.
  - Pushes updated data back to Firebase.
  - Uses Redis for fast cache checking and avoids duplicate writes.

- `firebase_manager.py` abstracts all Firebase operations.
- `redis_manager.py` provides read/write access to Redis and manages keys for players/projections.
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

The script:
- Starts listening to Firebase changes.
- Fetches new projections via API.
- Updates Firebase with cleaned, merged player data.
- Sleeps and loops again!

---

## 🧪 Future Features

- Multi-league support (NHL, NBA)
- Write queue for batching Firebase updates
- Webhook notification system for major changes
