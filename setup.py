"""Ibis BigQuery backend."""

import pathlib
import site
import sys
from typing import Dict

import setuptools

# See https://github.com/pypa/pip/issues/7953
site.ENABLE_USER_SITE = "--user" in sys.argv[1:]

# Package metadata.

name = "ibis-bigquery"
description = "Ibis BigQuery backend"

# Should be one of:
# 'Development Status :: 3 - Alpha'
# 'Development Status :: 4 - Beta'
# 'Development Status :: 5 - Production/Stable'
release_status = "Development Status :: 5 - Production/Stable"

package_root = pathlib.Path(__file__).parent

version_dict: Dict[str, str] = {}
with open(package_root / "ibis_bigquery" / "version.py") as fp:
    exec(fp.read(), version_dict)
version = version_dict["__version__"]

readme_filename = package_root / "README.rst"
with open(readme_filename, encoding="utf-8") as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name=name,
    version=version,
    description=description,
    long_description=readme,
    author="Ibis Contributors",
    maintainer="Tim Swast",
    maintainer_email="swast@google.com",
    url="https://github.com/ibis-project/ibis-bigquery",
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "ibis-framework >=2.0.0,<4.0.0dev",
        "db-dtypes>=0.3.0,<2.0.0dev",
        "google-cloud-bigquery >=1.12.0,<4.0.0dev",
        "google-cloud-bigquery-storage >=1.0.0,<3.0.0dev",
        "packaging >= 17.0",
        "pyarrow >=1.0.0,<10.0.0dev",
        "pydata-google-auth",
        "sqlalchemy>=1.4,<2.0",
    ],
    classifiers=[
        release_status,
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ],
    license="Apache 2.0",
    entry_points={"ibis.backends": ["bigquery = ibis_bigquery"]},
)
