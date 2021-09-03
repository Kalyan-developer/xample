# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ''

setup(
    long_description=readme,
    name='xml_processing',
    version='1.0',
    description='Parse XML files',
    python_requires='==3.*,>=3.6.0',
    author='Anonymous',
    author_email='Anonymous@chubb.com',
    entry_points={"console_scripts": ["xml_processing = run:main"]},
    packages=['xml_processing.app'],
    package_dir={"": "."},
    install_requires=[
        'pyyaml==3.*,>=3.13.0', 'toolz==0.*,>=0.9.0', 'xmltodict==0.*,>=0.12.0'
    ],
)
