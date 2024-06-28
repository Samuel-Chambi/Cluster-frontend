#! /bin/bash

mkdir cluster
cd cluster

sudo yum update -y
sudo yum install git -y
echo "git version"
git --version

python --version

pip --version

pip install Flask
pip install virtualenv
pip install pyspark

git clone git@github.com:Samuel-Chambi/Cluster-frontend.git
cd Cluster-frontend

virtualenv -p python3.9 env