from setuptools import find_packages, setup

import dldemo

setup(
    name="dldemo",
    version=dldemo.__version__,
    packages=find_packages(exclude=['docs*', 'tests*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "core4 @ git+https://github.com/plan-net/core4.git#egg=core4",
        "openpyxl",
        "matplotlib",
        "bs4"
    ],
    extras_require={
        "tests": [
            "pytest",
            "pytest-timeout",
            "pytest-runner",
            "pytest-tornasync",
            "coverage"
        ]
    }
)