import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))


def get_package_name(line):
    if line.startswith('-e'):
        pkgname = line[line.find('=') + 1:]
        if '-' not in pkgname:
            return pkgname
        return pkgname[:pkgname.rfind('-')]
    return line

with open(os.path.join(here, 'VERSION')) as f:
    __version__ = f.read().strip()

with open(os.path.join(here, 'requirements.txt')) as f:
    required = [get_package_name(line) for line in f.read().splitlines()]

with open(os.path.join(here, 'README.md')) as f:
    long_description = f.read()


extra_files = []
extra_files.append(os.path.join(here, 'LICENSE'))
extra_files.append(os.path.join(here, 'requirements.txt'))
extra_files.append(os.path.join(here, 'VERSION'))


setup(
    name='rdmfs',
    version=__version__,

    description='A FUSE library for GakuNin RDM',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/RCOSDP/rdmfs',

    # Author details
    author='RCOSDP',
    # Choose your license
    license='BSD3',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: BSD License',
        'Topic :: Utilities'
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(),
    incude_package_data=True,
    package_data={'': extra_files},

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=required,

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'rdmfs=rdmfs.__main__:main',
        ],
    },
)
