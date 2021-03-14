import inotify.adapters
from typing import Callable
from app.FileWatcher import FileWatcher
from app.Logger import getLogger

logger = getLogger("FileWatcher")


class FileWatcher2(FileWatcher):
    def __init__(self):
        super(FileWatcher, self).__init__()
        self._on_file_added = dict()

    def on_file_added(self, path: str, callback: Callable[[str, str], None]):
        self._on_file_added[path] = callback

    def run(self):
        i = inotify.adapters.Inotify()

        for path in self._on_file_added:
            logger.info("WATCHING %s" % path)
            i.add_watch(path)

        for event in i.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            if is_new_file(type_names) and self._on_file_added[path] is not None:
                self._on_file_added[path]("%s/%s" % (path, filename))
            else:
                logger.info("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}".format(path, filename, type_names))


def is_new_file(type_names: list) -> bool:
    if "IN_ISDIR" in type_names:
        return False

    # True if copied or moved
    return "IN_MOVED_TO" in type_names or "IN_CLOSE_WRITE" in type_names
