from datetime import datetime
import json
import sys
import logging
import errno
import time
from typing import Optional, Union, List, Dict, AsyncGenerator, Any

import pyfuse3
from cacheout import Cache
from osfclient import OSF
from osfclient.models import Storage, Project, File, Folder


log = logging.getLogger(__name__)
FILE_ATTRIBUTE_CACHE_TTL = 60 # 1 minute
LIST_CACHE_TTL = 180 # 3 minutes


def fromisoformat(datestr):
    datestr = datestr.replace('Z', '+00:00')
    return int(datetime.fromisoformat(datestr).timestamp() * 1e9)


class BaseInode:
    """The class for managing single inode."""
    def __init__(self, id: int):
        if not isinstance(id, int):
            raise ValueError('Invalid inode id: {}'.format(id))
        self.id = id
        self.removed = False

    def __str__(self) -> str:
        return f'<{self.__class__.__name__} [id={self.id}, path={self.path}]>'

    async def refresh(self, context: 'Inodes', force=False):
        log.debug(f'nothing to refresh: inode={self.id}')

    def invalidate(self, name: Optional[str] = None):
        pass

    def remove(self):
        self.removed = True

    @property
    def parent(self) -> Optional['BaseInode']:
        raise NotImplementedError

    @property
    def storage(self) -> 'StorageInode':
        raise NotImplementedError

    @property
    def object(self):
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def size(self) -> Optional[Union[int, str]]:
        return None

    def has_children(self) -> bool:
        return False

    @property
    def date_created(self) -> Optional[str]:
        return None

    @property
    def date_modified(self) -> Optional[str]:
        return None

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def display_path(self) -> str:
        raise self.path

    @property
    def can_create(self) -> bool:
        return False

    @property
    def can_move(self) -> bool:
        return False


class ProjectInode(BaseInode):
    """The class for managing single project inode."""
    def __init__(self, id: int, project: Project):
        super(ProjectInode, self).__init__(id)
        self.project = project

    @property
    def parent(self) -> Optional[BaseInode]:
        return None

    @property
    def storage(self):
        raise ValueError('Project inode does not have storage')

    @property
    def object(self):
        return self.project

    @property
    def name(self):
        return self.project.title

    def has_children(self):
        return True

    @property
    def path(self):
        return f'/{self.project.id}/'

    @property
    def display_path(self):
        return '/'


class StorageInode(BaseInode):
    """The class for managing single storage inode."""
    def __init__(self, id: int, project: ProjectInode, storage: Storage):
        super(StorageInode, self).__init__(id)
        self.project = project
        self._storage = storage

    @property
    def parent(self) -> Optional[BaseInode]:
        return self.project

    @property
    def storage(self):
        return self

    @property
    def object(self):
        return self._storage

    @property
    def name(self):
        return self._storage.name

    def has_children(self):
        return True

    @property
    def path(self):
        return f'{self.parent.path}{self._storage.name}/'

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self._storage.name}/'

    @property
    def can_create(self):
        return True


class BaseFileInode(BaseInode):
    """The class for managing single file inode."""
    _updated: Optional[Union[File, Folder]]
    _last_loaded: float
    _updated_name: Optional[str]

    def __init__(
        self,
        id: int,
        parent: BaseInode,
    ):
        super(BaseFileInode, self).__init__(id)
        self._parent = parent
        self._updated = None
        self._last_loaded = time.time()
        self._updated_name = None

    def invalidate(self, name: Optional[str] = None):
        self._last_loaded = None
        self._updated_name = name

    async def refresh(self, context: 'Inodes', force=False):
        expired = self._last_loaded is None or \
            time.time() - self._last_loaded > FILE_ATTRIBUTE_CACHE_TTL
        if not force and not self._is_new_file and not expired:
            return
        child = await self._get_child_by_name(self.name)
        if child is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        self._validate(child)
        self._updated = child
        self._updated_name = None
        self._last_loaded = time.time()

    @property
    def parent(self) -> Optional[BaseInode]:
        return self._parent

    @property
    def storage(self):
        return self._parent.storage

    @property
    def object(self):
        return self._latest

    @property
    def name(self) -> Optional[str]:
        return self._updated_name or self._latest.name

    @property
    def _latest(self):
        return self._updated or self._original

    @property
    def date_created(self) -> Optional[str]:
        return self._latest.date_created

    @property
    def date_modified(self) -> Optional[str]:
        return self._latest.date_modified

    @property
    def _path(self) -> str:
        try:
            return self._latest.osf_path
        except AttributeError:
            return self._latest.path

    @property
    def path(self):
        path = self._path if not self._path.startswith('/') else self._path[1:]
        return f'{self.storage.path}{path}'

    @property
    def can_move(self):
        return True

    def _validate(self, object: Union[File, Folder]):
        raise NotImplementedError

    @property
    def _original(self) -> Any:
        raise NotImplementedError

    @property
    def _is_new_file(self) -> bool:
        raise NotImplementedError

    async def _get_child_by_name(self, name: str) -> Optional[Union[File, Folder]]:
        async for child in self._parent.object.children:
            log.debug(f'searching({name})... child: {child}, name: {child.name}')
            if child.name == name:
                return child
        return None


class FolderInode(BaseFileInode):
    """The class for managing single folder inode."""
    def __init__(self, id: int, parent: BaseInode, folder: Folder):
        super(FolderInode, self).__init__(id, parent)
        self.folder = folder

    @property
    def _original(self):
        return self.folder

    def has_children(self):
        return True

    @property
    def _is_new_file(self):
        return False

    def _validate(self, object: Union[File, Folder]):
        pass

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self.name}/'

    @property
    def can_create(self) -> bool:
        return True


class NewFile:
    """Dummy class for new file."""
    def __init__(self, parent: BaseInode, name: str):
        self.parent = parent
        self.name = name

    @property
    def path(self):
        return f'{self.parent.path}{self.name}'


class FileInode(BaseFileInode):
    """The class for managing single file inode."""
    _invalidated: bool

    def __init__(
        self,
        id: int,
        parent: BaseInode,
        file: Union[File, NewFile],
    ):
        super(FileInode, self).__init__(id, parent)
        self.file = file
        self._invalidated = False

    def invalidate(self, name: Optional[str] = None):
        super(FileInode, self).invalidate(name=name)
        self._invalidated = True

    async def refresh(self, context: 'Inodes', force=False):
        if self._is_new_file and not self._invalidated:
            # New file and not invalidated(=not written) -> no need to refresh
            return
        await super(FileInode, self).refresh(context, force=force)
        if self._is_new_file:
            self.file = self._updated

    @property
    def _original(self):
        return self.file

    @property
    def _is_new_file(self):
        return isinstance(self.file, NewFile)

    @property
    def name(self):
        if self._is_new_file:
            return self.file.name
        return super(FileInode, self).name

    @property
    def path(self):
        if self._is_new_file:
            return self.file.path
        return super(FileInode, self).path

    @property
    def date_created(self):
        if self._is_new_file:
            return None
        return super(FileInode, self).date_created

    @property
    def date_modified(self):
        if self._is_new_file:
            return None
        return super(FileInode, self).date_modified

    @property
    def size(self) -> Optional[Union[int, str]]:
        if not hasattr(self._latest, 'size'):
            return None
        if self._latest.size is None:
            return None
        if type(self._latest.size) != int and type(self._latest.size) != str:
            return None
        return self._latest.size

    def _validate(self, object: Union[File, Folder]):
        if isinstance(object, Folder):
            raise pyfuse3.FUSEError(errno.EISDIR)

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self.name}'


class ChildRelation:
    """The class for managing child relations."""
    def __init__(self, parent: BaseInode, children: List[BaseInode]):
        """Initialize ChildRelation object."""
        self.parent = parent
        self.children = children


class Inodes:
    """The class for managing multiple inodes."""
    INODE_DUMMY = -1
    osf: OSF
    project: str
    osfproject: Optional[Project]
    _inodes: Dict[int, BaseInode]
    _child_relations: Cache

    def __init__(self, osf: OSF, project: str):
        """Initialize Inodes object."""
        super(Inodes, self).__init__()
        self.osf = osf
        self.project = project
        self.osfproject = None
        self.offset_inode = pyfuse3.ROOT_INODE + 1
        self._inodes = {}
        self._child_relations = Cache(maxsize=256, ttl=LIST_CACHE_TTL, timer=time.time, default=None)

    async def _get_osfproject(self):
        """Get OSF project object."""
        if self.osfproject is not None:
            return self.osfproject
        self.osfproject = await self.osf.project(self.project)
        return self.osfproject

    def register(self, parent_inode: BaseInode, name: str):
        """Register new inode."""
        log.debug(f'register: path={parent_inode}, name={name}')
        newfile = NewFile(parent_inode, name)
        inode = self._get_object_inode(parent_inode, newfile)
        log.debug(f'registered: inode={inode}')
        return inode

    def invalidate(self, inode: Union[int, BaseInode], name: Optional[str] = None):
        """Invalidate inode.

        If inode is integer, it is treated as inode number."""
        log.debug(f'invalidate: inode={inode}, name={name}')
        if isinstance(inode, int):
            if inode not in self._inodes:
                raise ValueError('Unexpected inode: {}'.format(inode))
            inode = self._inodes[inode]
        self._child_relations.delete(inode.id)
        inode.invalidate(name=name)

    async def get(self, inode_num: int) -> Optional[BaseInode]:
        """Find inode by inode number."""
        if inode_num in self._inodes:
            return self._inodes[inode_num]
        if inode_num == pyfuse3.ROOT_INODE:
            project = await self._get_osfproject()
            inode = ProjectInode(pyfuse3.ROOT_INODE, project)
            self._inodes[inode_num] = inode
            return inode
        return None

    async def find_by_name(self, parent: BaseInode, name: str) -> Optional[BaseInode]:
        """Find inode by name."""
        if not parent.has_children():
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        cache: Optional[ChildRelation] = self._child_relations.get(parent.id)
        if cache is not None:
            # Use cache
            for child in cache.children:
                if child.name == name:
                    return child
        # Request to GRDM
        found = None
        children: List[BaseInode] = []
        async for child in self.get_children_of(parent):
            children.append(self._get_object_inode(parent, child))
            log.debug(f'find_by_name: name={name}, child={child.name}')
            if child.name == name:
                found = child
        cache = ChildRelation(parent, children)
        self._child_relations.set(parent.id, cache)
        if found is not None:
            return self._get_object_inode(parent, found)
        # Find from new files
        return self._find_new_file_by_name(parent, name)

    async def get_children_of(self, parent: BaseInode) -> AsyncGenerator[Union[Storage, File, Folder], None]:
        """Get children of the parent inode."""
        log.debug(f'get_children_of: parent={parent}')
        if not parent.has_children():
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        if isinstance(parent, ProjectInode):
            project = parent.object
            async for storage in project.storages:
                yield storage
            return
        async for child in parent.object.children:
            yield child

    def _get_object_inode(self, parent: BaseInode, object: Union[Storage, File, Folder]) -> BaseInode:
        """Get inode for the object."""
        dummy_inode = self._create_object_inode(self.INODE_DUMMY, parent, object)
        for inode in self._inodes.values():
            if inode.removed:
                continue
            if inode.path == dummy_inode.path:
                return inode
        new_file_inode = self._find_new_file_by_name(parent, dummy_inode.name)
        if new_file_inode is not None:
            return new_file_inode
        # Register new inode
        new_inode = None
        for inode in range(self.offset_inode, sys.maxsize):
            if inode not in self._inodes:
                new_inode = inode
                break
        if new_inode is None:
            raise ValueError('Cannot allocate new inodes')
        self._inodes[new_inode] = r = self._create_object_inode(new_inode, parent, object)
        log.debug(f'new inode: inode={r}')
        return r

    def _create_object_inode(self, inode_num: int, parent: BaseInode, object: Union[Storage, File, Folder]) -> BaseInode:
        """Create inode object for the object."""
        if isinstance(object, Storage):
            return StorageInode(inode_num, parent, object)
        if isinstance(object, Folder):
            return FolderInode(inode_num, parent, object)
        return FileInode(inode_num, parent, object)

    def _find_new_file_by_name(self, parent: BaseInode, name: str) -> Optional[BaseInode]:
        """Find new file by name."""
        for inode in self._inodes.values():
            if inode.removed:
                continue
            if not isinstance(inode, FileInode):
                continue
            if inode.parent != parent:
                continue
            if inode._is_new_file and inode.name == name:
                return inode
        return None
