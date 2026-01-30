from setuptools import setup, find_packages

setup(
    name="hdbcv2dsp",
    version="0.1.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
)