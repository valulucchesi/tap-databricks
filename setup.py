#!/usr/bin/env python
from setuptools import setup

setup(
      name="tap-databricks",
      version="0.1.0",
      description="Singer.io tap for extracting databricks usage.",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_databricks"],
      install_requires=[
            "singer-python>=5.0.12",
            "requests",
            "pendulum"
      ],
      entry_points="""
    [console_scripts]
    tap-databricks=tap_databricks:main
    """,
      packages=["tap_databricks"],
      package_data = {
            "schemas": ["tap_databricks/schemas/*.json"]
      },
      include_package_data=True,
)
