import os
import re
from typing import Optional, IO
from .inode import BaseInode

class Whitelist:
    def __init__(self, file: IO[str]):
        self.patterns = []
        comment_pattern = re.compile(r'^\s*\#.*')
        for line in file.readlines():
            if comment_pattern.match(line):
                continue
            self.patterns.append(re.compile(line.strip()))

    def includes(self, inode: BaseInode, name: Optional[str]=None):
        base = inode.display_path
        if name is not None:
            path = os.path.join(base, name)
        else:
            path = base
        return any([pattern.match(path) is not None for pattern in self.patterns])
