import io
import re

from setuptools import setup, find_packages


with io.open('README.md', 'rt', encoding='utf8') as f:
    readme = f.read()

with io.open('scheduler/__init__.py', 'rt', encoding='utf8') as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)

setup(
    name='scheduler',
    version=version,
    author='Alex Brady',
    author_email='alex.brady.py@gmail.com',
    description='Shift scheduling application for takeaway drivers',
    long_description=readme,
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    packages=find_packages('.', exclude=['tests']),
    test_suite='tests',
    setup_requires=['pytest-runner'],
    install_requires=[
        'pandas==0.24.2',
        'numpy==1.18.1',
        'pandas_schema==0.3.5'
        ],
    tests_require=[
        'pytest',
    ],
    zip_safe=False,
    platforms='any',
)
