# Test Spark

This repo verifies that Spark is properly installed. It also verifies that new
packages can be installed automatically, in this the Delta Lake package is
installed and configured.



## Getting Started

### Prerequisites

To verify the spark installation the following requirements should be satisfied:

- Python 3.x should be installed
- Pip should be installed
- Spark and Hadoop should be installed and configured

## Steps to use

Simply run the following commands:

`python3 -m pip install pipenv`
`python3 -m pipenv install`
`python3 -m pipenv run test`

If all tests pass, Spark is properly configured and ready to be used. Moreover,
this repo can be used as a template with unit tests and proper directory
structure.

The repo can also be executed by running:

`python3 -m pipenv run start`

This command however will not run tests but instead it will run the functions
one after the other.

## Compatibility

This repo should work for both Linux/Mac and Windows, but it has been tested
with Windows (with WSL2).

To install Hadoop and Spark on Windows with WSL follow the steps presented in
[this post](https://kontext.tech/article/560/apache-spark-301-installation-on-linux-guide). 
It is also suggested to use the [WSL Remote extension for VS Code](https://code.visualstudio.com/docs/remote/wsl-tutorial)


