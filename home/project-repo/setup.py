from setuptools import find_packages, setup

setup(
    name="census-data",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "requests",
        "python-dotenv",
    ],
)
