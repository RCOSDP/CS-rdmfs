import errno
import logging
import re
from pyfuse3 import FUSEError


log = logging.getLogger(__name__)


def reraise_fuse_error(e):
    log.error('FUSE error: %s', e, exc_info=True)
    if isinstance(e, RuntimeError):
        # When the exception "RuntimeError: Response has status code 403 not (200,)" is raised,
        # raises FUSEError(errno.EACCES) instead of RuntimeError.
        message = str(e)
        status_code = re.match(r'.*Response has status code (\d+).*', message)
        if status_code and status_code.group(1) == '403':
            raise FUSEError(errno.EACCES)
        if status_code and status_code.group(1) == '404':
            raise FUSEError(errno.ENOENT)
    raise FUSEError(errno.EBADF)
