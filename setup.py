from io import open
from setuptools import setup, find_packages


def read(f):
    return open(f, "r").read()


setup(
    name="motordantic",
    version="0.2.1a1",
    packages=find_packages(exclude=("tests", "docs", "examples")),
    install_requires=[
        "pydantic>=1.10",
        "pymongo==4.1",
        "motor==3.0.0",
    ],
    description="Mongo ODM, based on motor+pydantic",
    author="bzdvdn",
    author_email="bzdv.dn@gmail.com",
    url="https://github.com/bzdvdn/motordantic",
    license="MIT",
    python_requires=">=3.9",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
)
