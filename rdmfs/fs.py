from datetime import datetime
import os
import stat
import logging
import errno
from typing import Optional

import pyfuse3
from osfclient import exceptions as osf_exceptions
from .node import FileContext, flags_can_write
from .inode import Inodes, fromisoformat
from .filehandle import FileHandlers
from .exception import reraise_fuse_error
from .whitelist import Whitelist

log = logging.getLogger(__name__)

class RDMFileSystem(pyfuse3.Operations):
    def __init__(self, osf, project, dir_mode=0o755, file_mode=0o644,
                 uid=None, gid=None,
                 writable_whitelist: Optional[Whitelist]=None,
                 list_all_projects: bool=False):
        super(RDMFileSystem, self).__init__()
        self.inodes = Inodes(osf, project, list_all_projects=list_all_projects)
        self.file_handlers = FileHandlers()
        self.dir_mode = dir_mode
        self.file_mode = file_mode
        self.uid = uid or os.getuid()
        self.gid = gid or os.getgid()
        self.writable_whitelist = writable_whitelist

    async def getattr(self, inode_num, ctx=None):
        log.info('getattr: inode={inode}'.format(inode=inode_num))
        try:
            entry = pyfuse3.EntryAttributes()
            inode = await self.inodes.get(inode_num)
            if inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            await inode.refresh(self.inodes)
            if inode.has_children():
                entry.st_mode = (stat.S_IFDIR | self.dir_mode)
                entry.st_size = 0
            else:
                entry.st_mode = (stat.S_IFREG | self.file_mode)
                log.debug('getattr: name={}, size={}'.format(inode.name, inode.size))
                if inode.size is not None:
                    entry.st_size = int(inode.size)
                else:
                    entry.st_size = 0
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(inode):
                entry.st_mode = entry.st_mode & (~0o200)
            stamp = 0
            mstamp = stamp
            if inode.date_created is not None:
                stamp = fromisoformat(inode.date_created)
            if inode.date_modified is not None:
                mstamp = fromisoformat(inode.date_modified)
            entry.st_atime_ns = stamp
            entry.st_ctime_ns = stamp
            entry.st_mtime_ns = mstamp
            entry.st_gid = self.gid
            entry.st_uid = self.uid
            entry.st_ino = inode.id
            entry.entry_timeout = 5
            entry.attr_timeout = 5
            log.debug('getattr: inode={}, result={}'.format(inode, entry))
            return entry
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def setattr(self, inode_num, attr, fields, fh, ctx=None):
        log.info('setattr: inode={inode}, attr={attr}, fh={fh}'.format(
            inode=inode_num, attr=attr, fh=fh
        ))
        try:
            log.warning('setattr not supported')
            return await self.getattr(inode_num)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def lookup(self, parent_inode_num, bname, ctx=None):
        name = bname.decode('utf8')
        log.info('lookup parent_inode={parent_inode}, name={name}'.format(
            parent_inode=parent_inode_num, name=name
        ))
        try:
            parent_inode = await self.inodes.get(parent_inode_num)
            if parent_inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            log.debug('lookup: parent_inode_obj={}'.format(parent_inode))
            inode = await self.inodes.find_by_name(parent_inode, name)
            if inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            return await self.getattr(inode.id)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def opendir(self, inode_num, ctx):
        log.info('opendir: inode={inode}'.format(inode=inode_num))
        try:
            inode = await self.inodes.get(inode_num)
            log.debug('opendir: inode_obj={}'.format(inode))
            if inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            await inode.refresh(self.inodes)
            return self.file_handlers.get_node_fh(FileContext(self, inode))
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def readdir(self, fh, start_id, token):
        log.info('readdir: fh={fh}, start_id={start_id}'.format(
            fh=fh, start_id=start_id
        ))
        try:
            folder = self.file_handlers.find_node_by_fh(fh)
            assert folder is not None
            await folder.readdir(start_id, token)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)
        return

    async def setxattr(self, inode_num, name, value, ctx):
        log.info('setxattr: inode={inode}, name={name}, value={value}'.format(
            inode=inode_num, name=name, value=value
        ))
        if inode_num != pyfuse3.ROOT_INODE or name != b'command':
            raise pyfuse3.FUSEError(errno.ENOTSUP)

        if value == b'terminate':
            pyfuse3.terminate()
        else:
            raise pyfuse3.FUSEError(errno.EINVAL)

    async def open(self, inode_num, flags, ctx):
        log.info('open: inode={inode}, flags={flags}'.format(inode=inode_num, flags=flags))
        try:
            inode = await self.inodes.get(inode_num)
            if inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if flags_can_write(flags) and getattr(inode, 'readonly', False):
                raise pyfuse3.FUSEError(errno.EACCES)
            if flags_can_write(flags) and \
                self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(inode):
                raise pyfuse3.FUSEError(errno.EACCES)
            await inode.refresh(self.inodes)
            return pyfuse3.FileInfo(fh=self.file_handlers.get_node_fh(
                FileContext(self, inode, flags)
            ))
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def create(self, parent_inode_num, name, mode, flags, ctx):
        sname = name.decode('utf8')
        log.info('create: parent_inode={inode} name={sname}'.format(
            inode=parent_inode_num, sname=sname
        ))
        try:
            parent_inode = await self.inodes.get(parent_inode_num)
            if parent_inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            log.info('create: parent_path={}'.format(parent_inode.path))
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(parent_inode, sname):
                raise pyfuse3.FUSEError(errno.EACCES)
            if not parent_inode.can_create:
                raise pyfuse3.FUSEError(errno.EACCES)
            inode = await self.inodes.register(parent_inode, sname)
            entry = pyfuse3.EntryAttributes()
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_size = 0
            stamp = int(datetime.now().timestamp() * 1e9)
            mstamp = stamp
            entry.st_atime_ns = stamp
            entry.st_ctime_ns = stamp
            entry.st_mtime_ns = mstamp
            entry.st_gid = os.getgid()
            entry.st_uid = os.getuid()
            entry.st_ino = inode.id
            entry.entry_timeout = 5
            entry.attr_timeout = 5

            return (
                pyfuse3.FileInfo(fh=self.file_handlers.get_node_fh(
                    FileContext(self, inode, flags)
                )),
                entry
            )
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def read(self, fh, off, size):
        log.info('read: fh={fh}, off={off}, size={size}'.format(
            fh=fh, off=off, size=size
        ))
        try:
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            return await file_.read(off, size)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def write(self, fh, off, buf):
        log.info('write: fh={fh}, off={off}'.format(fh=fh, off=off))
        try:
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            return await file_.write(off, buf)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def flush(self, fh):
        log.info('flush: fh={fh}'.format(fh=fh))
        try:
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            await file_.flush()
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def release(self, fh):
        log.info('release: fh={fh}'.format(fh=fh))
        try:
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            await file_.close()
            self.file_handlers.release_fh(fh)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def releasedir(self, fh):
        log.info('releasedir: fh={fh}'.format(fh=fh))
        try:
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            await file_.close()
            self.file_handlers.release_fh(fh)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def mkdir(self, parent_inode_num, name, mode, ctx):
        log.info('mkdir: parent_inode={inode}, name={name}'.format(
            inode=parent_inode_num, name=name
        ))
        try:
            inode = await self.inodes.get(parent_inode_num)
            if inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            sname = name.decode('utf8')
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(inode, sname + '/'):
                raise pyfuse3.FUSEError(errno.EACCES)
            if not inode.can_create:
                raise pyfuse3.FUSEError(errno.EACCES)
            new_folder = await inode.object.create_folder(sname)
            new_attr = await self.lookup(parent_inode_num, name)
            log.info('mkdir: folder={}, attr={}'.format(new_folder, new_attr))
            return new_attr
        except osf_exceptions.FolderExistsException:
            raise pyfuse3.FUSEError(errno.EEXIST)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def rmdir(self, parent_inode_num, name, ctx):
        log.info('rmdir: parent_inode={inode}, name={name}'.format(
            inode=parent_inode_num, name=name
        ))
        try:
            parent_inode = await self.inodes.get(parent_inode_num)
            if parent_inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            target = None
            sname = name.decode('utf8')
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(parent_inode, sname + '/'):
                raise pyfuse3.FUSEError(errno.EACCES)
            target = await self.inodes.find_by_name(parent_inode, sname)
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if not target.has_children():
                log.info('attempt to rmdir file: {}'.format(target))
                raise pyfuse3.FUSEError(errno.ENOTDIR)
            async for _ in self.inodes.get_children_of(target):
                raise pyfuse3.FUSEError(errno.ENOTEMPTY)
            log.info('rmdir: folder={}'.format(target))
            await target.object.remove()
            target.remove()
            self.inodes.invalidate(parent_inode)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def rename(self, parent_inode_old_num, name_old, parent_inode_new_num, name_new, flags, ctx):
        log.info('rename: parent_inode_old={inode_old}, name_old={name_old}, parent_inode_new={inode_new}, name_new={name_new}'.format(
            inode_old=parent_inode_old_num, name_old=name_old, inode_new=parent_inode_new_num, name_new=name_new
        ))
        try:
            parent_inode_old = await self.inodes.get(parent_inode_old_num)
            if parent_inode_old is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            parent_inode_new = await self.inodes.get(parent_inode_new_num)
            if parent_inode_new is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            target_old = None
            sname_old = name_old.decode('utf8')
            target_old = await self.inodes.find_by_name(parent_inode_old, sname_old)
            if target_old is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            sname_new = name_new.decode('utf8')
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(parent_inode_old, sname_old):
                raise pyfuse3.FUSEError(errno.EACCES)
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(parent_inode_new, sname_new):
                raise pyfuse3.FUSEError(errno.EACCES)
            if not target_old.can_move:
                raise pyfuse3.FUSEError(errno.EACCES)
            if not parent_inode_new.can_create:
                raise pyfuse3.FUSEError(errno.EACCES)
            await target_old.refresh(self.inodes, force=True)
            target_old_obj = target_old.object
            storage_new = parent_inode_new.storage.object
            store_new = parent_inode_new.object
            if sname_old != sname_new:
                if target_old.has_children():
                    await target_old_obj.move_to(storage_new, store_new, to_foldername=sname_new)
                else:
                    await target_old_obj.move_to(storage_new, store_new, to_filename=sname_new)
            else:
                await target_old_obj.move_to(storage_new, store_new)
            target_old.set_parent(parent_inode_new)
            log.info('move: src={}, dest={}, destname={}'.format(
                target_old, store_new, sname_new
            ))
            self.inodes.invalidate(target_old, name=sname_new)
            self.inodes.invalidate(parent_inode_old)
            self.inodes.invalidate(parent_inode_new)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)

    async def unlink(self, parent_inode_num, name, ctx):
        log.info('unlink: parent_inode={inode}, name={name}'.format(
            inode=parent_inode_num, name=name
        ))
        try:
            parent_inode = await self.inodes.get(parent_inode_num)
            if parent_inode is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            sname = name.decode('utf8')
            if self.writable_whitelist is not None and \
                not self.writable_whitelist.includes(parent_inode, sname):
                raise pyfuse3.FUSEError(errno.EACCES)
            target = await self.inodes.find_by_name(parent_inode, sname)
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            log.info('unlink: file={}'.format(target))
            await target.refresh(self.inodes)
            await target.object.remove()
            target.remove()
            self.inodes.invalidate(parent_inode)
        except pyfuse3.FUSEError as e:
            raise e
        except BaseException as e:
            reraise_fuse_error(e)
