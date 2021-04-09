"""Ibis BigQuery backend."""
import os

import setuptools


BASE_PATH = os.path.abspath(os.path.dirname(__file__))


setuptools.setup(
    name='ibis-bigquery',
    description='Ibis BigQuery backend',
    long_description=open(os.path.join(BASE_PATH, 'README.md')).read(),
    url='https://github.com/ibis-project/ibis-bigquery',
    packages=setuptools.find_packages(),
    python_requires='>=3.7',
    install_requires=[
        'ibis-framework',  # TODO require ibis 2.0 when it's released
        'google-cloud-bigquery >=1.12.0,<3.0.0dev',
        'google-cloud-bigquery-storage >=1.0.0,<3.0.0dev',
        'pyarrow >=1.0.0,<4.0.0dev',
        'pydata-google-auth',
    ],
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
    ],
    license='Apache Software License',
)
