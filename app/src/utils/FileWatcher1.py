
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.Logger import getLogger
from typing import Callable
from src.utils.FileWatcher import FileWatcher
from src.utils.file_utils import get_path
logger = getLogger("Watcher")


class FileWatcher1(FileWatcher):

    def __init__(self):
        super(FileWatcher, self).__init__()
        self.observer = Observer()
        self._on_file_added = dict()
        self.event_handler = Handler(self._on_file_added)

    def on_file_added(self, path: str, on_input: Callable[[str], None]):
        logger.debug("on_file_added(%s, callback)" % path)
        self._on_file_added[path] = on_input

    def run(self):
        for path in self._on_file_added:
            logger.info("WATCHING %s" % path)
            self.observer.schedule(self.event_handler, path, recursive=False)

        self.observer.start()

        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            logger.error("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):

    def __init__(self, callbacks: dict):
        super(Handler, self).__init__()
        self._on_file_added = callbacks

    def on_any_event(self, event):
        if event.is_directory:
            logger.debug("IS A DIR")
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            logger.debug("Received created event - %s." % event.src_path)

        elif event.event_type == 'modified':
            path = get_path(event.src_path)
            logger.debug("Received modified event - %s." % event.src_path)
            if path in self._on_file_added:
                self._on_file_added[path](event.src_path)
            else:
                logger.warning("No watcher for %s" % path)

        elif event.event_type == 'moved':
            logger.debug("Received move event - %s." % event.src_path)

        else:
            logger.debug("Received %s event - %s" % (event.event_type, event.src_path))
