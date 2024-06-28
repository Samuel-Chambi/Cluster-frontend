#! /bin/bash

sudo yum update -y
sudo yum install git -y

python --version

pip --version

virtualenv -p python3.9 env
source env/bin/activate
pip install Flask
pip install pyspark
