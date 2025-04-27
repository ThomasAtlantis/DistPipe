import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="distpipe",
    version="0.0.1",
    author="Shangyu Liu",
    author_email="liushangyu@sjtu.edu.cn",
    description="DistPipe is a distributed framework to implement device-cloud collaborative workflow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ThomasAtlantis/DistPipe",
    python_requires='>=3.7',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)