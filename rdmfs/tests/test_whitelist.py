import pytest
from mock import MagicMock
import io

from rdmfs.whitelist import Whitelist


def test_whitelist():
    wl = Whitelist(io.StringIO('^test_1$\n ^test_2$\n^test_2\\/.*\n# ^test_3'))

    inode = MagicMock()
    inode.display_path = 'test_1'
    assert wl.includes(inode)

    inode.display_path = 'test_1/subdirectory'
    assert not wl.includes(inode)

    inode.display_path = 'test_2'
    assert wl.includes(inode)
    inode.display_path = 'test_2/subdirectory'
    assert wl.includes(inode)

    inode.display_path = 'test_3'
    assert not wl.includes(inode)

