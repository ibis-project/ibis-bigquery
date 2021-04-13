"""Ibis BigQuery backend."""

import pathlib

import setuptools

# Package metadata.

name = "ibis-bigquery"
description = "Ibis BigQuery backend"

# Should be one of:
# 'Development Status :: 3 - Alpha'
# 'Development Status :: 4 - Beta'
# 'Development Status :: 5 - Production/Stable'
release_status = "Development Status :: 4 - Beta"

package_root = pathlib.Path(__file__).parent

version = {}
with open(package_root / "ibis_bigquery" / "version.py") as fp:
    exec(fp.read(), version)
version = version["__version__"]

readme_filename = package_root / "README.rst"
with open(readme_filename, encoding="utf-8") as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name=name,
    version=version,
    description=description,
    long_description=readme,
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
    classifiers=[
        release_status,
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
    ],
    license='Apache 2.0',
)
