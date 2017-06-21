#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name = "redpanda",
    version = "0.1.0",
    license = "Apache",
    description = "Provides panas-like DataFrame objects whose states are "
                  "completely in Redis",
    packages = ["redpanda"],
    install_requires = ["redis"]
)
