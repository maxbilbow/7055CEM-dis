from typing import Callable


class FileWatcher:

    def __init__(self):
        pass

    def on_file_added(self, path: str, on_input: Callable[[str], None]):
        pass

    def run(self):
        pass
