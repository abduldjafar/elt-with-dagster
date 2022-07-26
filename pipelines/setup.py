import setuptools

setuptools.setup(
    name="pipelines",
    packages=setuptools.find_packages(exclude=["pipelines_tests"]),
    install_requires=[
        "dagster==0.14.20",
        "dagit==0.14.20",
        "pytest",
    ],
)
