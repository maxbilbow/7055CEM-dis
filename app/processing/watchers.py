from app.config import Config
from app.Logger import getLogger
from app.file_utils import copy_file

DATA_IN = Config.get("path.data.in")
DATA_OUT = Config.get("path.data.out")
PROCESSING_RAW = Config.get("path.data._processing.raw")
PROCESSING_READY = Config.get("path.data._processing.ready")

logger = getLogger("pre-process")


def is_valid(path: str) -> bool:
    if not path.endswith(".csv"):
        logger.info("File not valid for data _processing: %s" % path)
        return False
    return True


def on_data_in(path: str):
    if not is_valid(path):
        return

    copy_file(path, PROCESSING_RAW)


def on_data_raw(path: str):
    if not is_valid(path):
        return

    copy_file(path, PROCESSING_READY)


def on_data_ready(path: str):
    if not is_valid(path):
        return

    copy_file(path, DATA_OUT)


def add_watchers():
    import os
    if os.name == "nt":
        logger.info("USING windows watcher")
        from app.FileWatcher1 import FileWatcher1
        FileWatcher = FileWatcher1
    else:
        logger.info("USING linux watcher")
        from app.FileWatcher2 import FileWatcher2
        FileWatcher = FileWatcher2

    fw = FileWatcher()
    fw.on_file_added(DATA_IN, on_data_in)
    fw.on_file_added(PROCESSING_RAW, on_data_raw)
    fw.on_file_added(PROCESSING_READY, on_data_ready)
    fw.run()

