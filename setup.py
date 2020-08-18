from setuptools import setup, find_packages


setup(
    name="ct_snippets",
    version="1.0",
    packages=find_packages(include=["ct_snippets", "ct_snippets.*"]),
)
