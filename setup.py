# -*- coding: utf-8 -*-
import setuptools

with open("README.MD", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dataflow",
    version="1.0.0",
    author="joinsunsoft",
    author_email="inthirties.liu@hotmail.com",
    description="Dataflow is an open-source Python microservice framework that simplifies the development of stand-alone, production-grade Spring applications.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gohutool/dataflow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache 2.0 License",
        "Operating System :: OS Independent",
    ],
)
