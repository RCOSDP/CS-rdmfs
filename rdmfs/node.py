import logging
import os
import pyfuse3
from aiofile import AIOFile, Reader
import aiofiles
from osfclient.models import File
from .inode import BaseInode, NewFile


log = logging.getLogger(__name__)


def flags_can_write(flags):
    if flags & 0x03 == os.O_RDWR:
        return True
    if flags & 0x03 == os.O_WRONLY:
        return True
    return False


class FileContext:
    def __init__(self, context, inode: BaseInode, flags=None):
        self.context = context
        self.inode = inode
        self.current_id = None
        self.aiterator = None
        self.buffer = None
        self.bufferfile = None
        self.flags = flags
        self.flush_count = 0

    async def _flush(self, fp):
        if isinstance(self.inode.object, File):
            await self.inode.object.update(fp)
            return
        storage = self.inode.storage
        if not self.inode.display_path.startswith(storage.display_path):
            raise ValueError('Storage path {0} does not start with {1}'.format(
                self.inode.display_path, storage.display_path
            ))
        relative_path = self.inode.display_path[len(storage.display_path):]
        log.debug('flush: storage={storage}, path={path}'.format(
            storage=storage, path=relative_path
        ))
        await storage.object.create_file(relative_path, fp)

    async def _write_to(self, fp):
        if isinstance(self.inode.object, NewFile):
            return
        await self.inode.object.write_to(fp)

    async def _invalidate(self):
        self.context.inodes.invalidate(self.inode)

    def is_write(self):
        return flags_can_write(self.flags)

    def is_new_file(self):
        return isinstance(self.inode.object, NewFile)

    async def close(self):
        if self.buffer is None and self.is_new_file() and self.is_write():
            await self._ensure_buffer()
        await self.flush()
        self.buffer = None
        await self._invalidate()

    async def read(self, offset, size):
        f = await self._ensure_buffer()
        f.seek(offset)
        return f.read(size)

    async def write(self, offset, buf):
        f = await self._ensure_buffer()
        f.seek(offset)
        f.write(buf)
        return len(buf)

    async def flush(self):
        if self.bufferfile is None:
            return
        self.flush_count += 1
        self.bufferfile.close()
        self.bufferfile = None
        if not self.is_write():
            return
        async with AIOFile(self.buffer, 'rb') as afp:
            reader = Reader(afp, chunk_size=4096)
            #reader.mode = 'rb'
            #reader.peek = lambda x=None: True
            await self._flush(reader)
        os.remove(self.buffer)

    async def readdir(self, start_id, token):
        if self.aiterator is None:
            self.current_id = 0
            self.aiterator = self.__aiter__()
        if start_id != self.current_id:
            return None
        self.current_id += 1
        try:
            object = await self.aiterator.__anext__()
            log.debug('Result: name={}, inode={}'.format(object.name, self.inode))
            child_inode = await self.context.inodes.find_by_name(self.inode, object.name)
            log.info('Result: name={}, inode={}, child={}'.format(object.name, self.inode, child_inode))
            pyfuse3.readdir_reply(
                token, object.name.encode('utf8'),
                await self.context.getattr(child_inode.id),
                self.current_id)
        except StopAsyncIteration:
            log.info('Finished, inode={}'.format(self.inode))
            return None

    async def _ensure_buffer(self):
        if self.bufferfile is not None:
            return self.bufferfile
        if self.buffer is None:
            async with aiofiles.tempfile.NamedTemporaryFile(delete=False) as f:
                await self._write_to(f)
                self.buffer = f.name
        mode = 'rb'
        if self.flags is None:
            pass
        elif self.flags & 0x03 == os.O_RDWR and self.flags & os.O_APPEND:
            mode = 'a+b'
        elif self.flags & 0x03 == os.O_RDWR:
            mode = 'r+b'
        elif self.flags & 0x03 == os.O_WRONLY and self.flags & os.O_APPEND:
            mode = 'ab'
        elif self.flags & 0x03 == os.O_WRONLY:
            mode = 'wb'
        log.info('buffer: file={2}, flags={0:08x}, mode={1}'.format(
            self.flags, mode, self.buffer
        ))
        self.bufferfile = open(self.buffer, mode)
        return self.bufferfile

    def __aiter__(self):
        return self._get_folders_and_files().__aiter__()

    async def _get_folders_and_files(self):
        if not self.inode.has_children():
            raise ValueError(f'File {self.inode.display_path} has no children')
        async for f in self.context.inodes.get_children_of(self.inode):
            yield f
