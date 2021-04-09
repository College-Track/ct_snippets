from setuptools import setup, find_packages
from setuptools.command.install import install
import atexit
import os.path


# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()


# class new_install(install):
#     def __init__(self, *args, **kwargs):
#         super(new_install, self).__init__(*args, **kwargs)
#         atexit.register(_post_install)


setup(
    name="ct_snippets",
    version="0.1",
    description="Code to manage common College Track data processes",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/college-track/ct_snippets",
    author="Baker Renneckar",
    author_email="brenneckar@collegetrack.org",
    license="MIT",
    packages=find_packages(include=["ct_snippets", "ct_snippets.*"]),
    install_requires=["pandas", "simple-salesforce", "reportforce"],
    # cmdclass={"install": new_install},
    include_package_data=True,
    # package_data={"ct_snippets": ["ct_snippets/college_track.mplstyle",]},
)

