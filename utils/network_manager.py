import os
import time
import threading
from db_utils import check_internet_connection, set_online_status, is_online

class NetworkManager:
    def __init__(self, ui_queue=None, poll_interval=15):
        self.ui_queue = ui_queue
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread = None
        self.host = os.getenv("DB_HOST", "100.24.75.156")
        self.port = int(os.getenv("DB_PORT", "3306"))

    def start(self):
        """Starts the connectivity polling thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stops the connectivity polling thread."""
        self._stop_event.set()

    def _poll_loop(self):
        while not self._stop_event.is_set():
            # Check connection using DB host to ensure actual DB reachability
            current_status = check_internet_connection(host=self.host, port=self.port, timeout=2)
            
            if current_status != is_online():
                set_online_status(current_status)
                
                # Notify UI if queue is provided
                if self.ui_queue:
                    color = "#4CAF50" if current_status else "#F44336"
                    text = "🟢 Online" if current_status else "🔴 Offline"
                    self.ui_queue.put(("connection_status", (color, text)))
            
            time.sleep(self.poll_interval)

    def force_check(self):
        """Performs an immediate connection check and updates status."""
        status = check_internet_connection(host=self.host, port=self.port, timeout=2)
        set_online_status(status)
        return status
