from setuptools import setup, find_packages
from setuptools.command.install import install
import atexit


def _post_install():
    import goosempl

    ct_snippets.copy_style()


class new_install(install):
    def __init__(self, *args, **kwargs):
        super(new_install, self).__init__(*args, **kwargs)
        atexit.register(_post_install)


setup(
    name="ct_snippets",
    version="1.0",
    packages=find_packages(include=["ct_snippets", "ct_snippets.*"]),
    install_requires=["pandas", "simple-salesforce", "matplotlib", "reportforce"],
    cmdclass={"install": new_install},
    package_data={"ct_snippets": ["ct_snippets/college_track.mplstyle",]},
)

