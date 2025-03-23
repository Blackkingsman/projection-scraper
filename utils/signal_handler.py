# signal_handler.py

import logging
import threading
import signal
import asyncio

class SignalHandler:
    def __init__(self, shutdown_event):
        self.shutdown_event = shutdown_event

    def register_signal_handlers(self):
        """
        Register signal handlers for graceful shutdown.
        """
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, sig, frame):
        """
        Handle the received signal for graceful shutdown.
        """
        logging.info(f"Received signal {sig}. Initiating graceful shutdown...")
        self.shutdown_event.set()  # Signal to shutdown

        # Start a new thread to handle blocking thread cleanup
        threading.Thread(target=self._cleanup_threads, daemon=True).start()

        # Stop the asyncio event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logging.info("Stopping the asyncio event loop...")
            loop.stop()

    def _cleanup_threads(self):
        """
        Wait for all threads to finish, but don't block the asyncio event loop.
        """
        for t in threading.enumerate():
            if t != threading.current_thread():
                logging.info(f"Waiting for thread {t.name} to finish...")
                t.join(timeout=5)

        logging.info("All threads finished. Graceful shutdown complete.")
