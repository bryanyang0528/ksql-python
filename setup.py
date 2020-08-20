#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Setup module """
import os

from setuptools import setup
from distutils.version import LooseVersion
import pip

if LooseVersion(pip.__version__) >= "10.0.0":
    from pip._internal.req import parse_requirements
else:
    from pip.req import parse_requirements

def get_install_requirements(path):
    content = open(os.path.join(os.path.dirname(__file__), path)).read()
    return [
        req
        for req in content.split("\n")
        if req != '' and not req.startswith('#')
    ]

# Get version from __init__.py file
VERSION = "0.10.1.1"

here = os.path.dirname(__file__)

# Get long description
README = open(os.path.join(os.path.dirname(__file__), "README.rst")).read()

setuptools_kwargs = {
    'install_requires': [
	    'requests',
		'six',
		'urllib3'
    ],
    'zip_safe': False,
}

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name="ksql",
    version=VERSION,
    description="A Python wrapper for the KSQL REST API",
    long_description=README,
    author="Bryan Yang",
    author_email="kenshin200528@gmail.com",
    url="https://github.com/bryanyang0528/ksql-python",
    license="MIT License",
    packages=[
        "ksql"
    ],
    include_package_data=True,
    platforms=['any'],
    extras_require={
        "dev": get_install_requirements("test-requirements.txt")
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    **setuptools_kwargs
)
