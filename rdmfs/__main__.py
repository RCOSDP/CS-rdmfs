from argparse import ArgumentParser
import asyncio
import logging
import os
import grp
import pwd
import re
import pyfuse3
import pyfuse3_asyncio
from rdmfs import fs, whitelist
from osfclient import cli


pyfuse3_asyncio.enable()

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--allow-other', action='store_true', default=False,
                        help='Enable allow_other option')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    parser.add_argument('-u', '--username', default=None,
                        help=('OSF username. Provide your password via '
                              'OSF_PASSWORD environment variable'))
    parser.add_argument('--base-url', default=None,
                        help='OSF API URL (Default is https://api.osf.io/v2/)')
    project_group = parser.add_mutually_exclusive_group()
    project_group.add_argument('-p', '--project', default=None,
                               help='OSF project ID')
    project_group.add_argument('--all-projects', action='store_true', default=False,
                               help='Mount all accessible projects under the root directory')
    parser.add_argument('--file-mode', default='0644',
                        help='Mode of files. default: 0644')
    parser.add_argument('--dir-mode', default='0755',
                        help='Mode of directories. default: 0755')
    parser.add_argument('--owner', default=None,
                        help='Owner(name or uid) of files. default: uid of current user')
    parser.add_argument('--group', default=None,
                        help='Group(name or gid) of files. default: gid of current user')
    parser.add_argument('--writable-whitelist', default=None,
                        help='Whitelist of writable files')
    return parser.parse_args()

def parse_mode(mode):
    m = re.match(r'^0([0-9]+)$', mode)
    if not m:
        raise ValueError(f'Unexpected mode: {mode}')
    return int(m.group(1), 8)

def parse_uid(uid):
    if uid is None:
        return uid
    m = re.match(r'^([0-9]+)$', uid)
    if m:
        return int(uid)
    return pwd.getpwnam(uid).pw_uid

def parse_gid(gid):
    if gid is None:
        return gid
    m = re.match(r'^([0-9]+)$', gid)
    if m:
        return int(gid)
    return grp.getgrnam(gid).gr_gid

def main():
    options = parse_args()
    init_logging(options.debug)

    placeholder_project = None
    if options.all_projects and options.project is None:
        placeholder_project = '__all_projects__'
        options.project = placeholder_project

    osf = cli._setup_osf(options)
    resolved_project = None if options.all_projects else options.project

    if placeholder_project is not None:
        options.project = None

    file_mode = parse_mode(options.file_mode)
    dir_mode = parse_mode(options.dir_mode)
    uid = parse_uid(options.owner)
    gid = parse_gid(options.group)
    writable_whitelist = None
    if options.writable_whitelist is not None:
        with open(options.writable_whitelist, 'r') as f:
            writable_whitelist = whitelist.Whitelist(f)
    if not options.all_projects and resolved_project is None:
        raise SystemExit('either --project or --all-projects must be specified')
    rdmfs = fs.RDMFileSystem(osf, resolved_project,
                             list_all_projects=options.all_projects,
                             file_mode=file_mode, dir_mode=dir_mode,
                             uid=uid, gid=gid,
                             writable_whitelist=writable_whitelist)
    fuse_options = set(pyfuse3.default_options)
    if options.allow_other:
        fuse_options.add('allow_other')
    fuse_options.add('fsname=rdmfs_asyncio')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(rdmfs, options.mountpoint, fuse_options)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(pyfuse3.main())
    except:
        pyfuse3.close(unmount=False)
        raise
    finally:
        loop.close()

    pyfuse3.close()


if __name__ == '__main__':
    main()
