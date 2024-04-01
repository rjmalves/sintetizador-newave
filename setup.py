from setuptools import setup, find_packages
from app import __version__

long_description = "sintetizador_newave"

requirements = []
with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()


setup(
    name="sintetizador_newave",
    version=__version__,
    author="Rogerio Alves",
    author_email="rogerioalves.ee@gmail.com",
    description="sintetizador_newave",
    long_description=long_description,
    install_requires=requirements,
    packages=find_packages(),
    py_modules=["main", "sintetizador"],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        sintetizador-newave=main:main
    """,
)
