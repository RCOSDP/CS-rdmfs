import pytest
import errno

from rdmfs.exception import reraise_fuse_error
from pyfuse3 import FUSEError


def test_reraise_fuse_error():
    with pytest.raises(FUSEError) as e:
        reraise_fuse_error(RuntimeError('Response has status code 403 not (200,)'))
    assert e.value.errno == errno.EACCES

    with pytest.raises(FUSEError) as e:
        reraise_fuse_error(RuntimeError('Response has status code 404 not (200,)'))
    assert e.value.errno == errno.ENOENT

    with pytest.raises(FUSEError) as e:
        reraise_fuse_error(RuntimeError('Response has status code 500 not (200,)'))
    assert e.value.errno == errno.EBADF
