# signal_handler.py

import logging
import threading
import signal
import asyncio

class SignalHandler:
    """
    Handles OS signals (e.g., SIGINT, SIGTERM) for graceful application shutdown.
    """

    def __init__(self, shutdown_event: threading.Event):
        """
        Initialize the SignalHandler with a shutdown event.

        Args:
            shutdown_event (threading.Event): Event to signal shutdown across threads.
        """
        self.shutdown_event = shutdown_event

    def register_signal_handlers(self) -> None:
        """
        Register signal handlers for SIGINT (Ctrl+C) and SIGTERM.
        """
        signal.signal(signal.SIGINT, self._handle_signal)  # Handle Ctrl+C
        signal.signal(signal.SIGTERM, self._handle_signal)  # Handle termination signals

    def _handle_signal(self, sig: int, frame) -> None:
        """
        Handle the received signal and initiate graceful shutdown.

        Args:
            sig (int): Signal number (e.g., SIGINT, SIGTERM).
            frame: Current stack frame (not used here).
        """
        logging.info(f"Received signal {sig}. Initiating graceful shutdown...")
        self.shutdown_event.set()  # Signal to shutdown

        # Stop the asyncio event loop if running
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                logging.info("Stopping the asyncio event loop...")
                loop.stop()
        except RuntimeError as e:
            logging.warning(f"Could not stop asyncio loop: {e}")

        # Start a new thread to handle blocking thread cleanup
        threading.Thread(target=self._cleanup_threads, daemon=True).start()

    def _cleanup_threads(self) -> None:
        """
        Wait for all threads to finish, but avoid blocking the asyncio event loop.

        This ensures that all non-daemon threads are joined before the application exits.
        """
        for t in threading.enumerate():
            if t != threading.current_thread():
                logging.info(f"Waiting for thread {t.name} to finish...")
                t.join(timeout=5)  # Wait up to 5 seconds for each thread to finish

        logging.info("All threads finished. Graceful shutdown complete.")
