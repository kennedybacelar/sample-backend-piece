import tempfile
import os
from pathlib import Path


class TemporaryDirectory(tempfile.TemporaryDirectory):
    """Same as tempfile.TemporaryDirectory except
    it returns with an object and has a new_file method for easy file creation.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc, value, tb):
        self.cleanup()

    @property
    def path(self):
        return Path(self.name)

    def new_file(self, content):
        fd, path = tempfile.mkstemp(dir=self.name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        os.close(fd)
        return path
