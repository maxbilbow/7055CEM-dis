import shutil
import os.path
import errno
from os.path import sep, dirname
from Logger import getLogger

logger = getLogger(__file__)

def make_dirs(path):
    try:
        os.makedirs(dirname(path))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise


def get_filename(path: str) -> str:
    bits = path.split("/")
    return bits[len(bits) - 1]


def get_path(src_path: str) -> str:
    index = src_path.rfind(sep)
    return src_path[0:index]


def copy_file(src_path: str, to_dir: str, overwrite = True):
    filename = get_filename(src_path)
    dest_path = "%s%s%s" % (to_dir, sep, filename)
    if os.path.isfile(dest_path):
        if overwrite:
            logger.warning("Overwriting file: %s" % dest_path)
        else:
            logger.warning("File already exists: %s" % dest_path)
            return
    logger.info("Copying %s to %s" % (src_path, to_dir))
    shutil.copyfile(src_path, dest_path)
