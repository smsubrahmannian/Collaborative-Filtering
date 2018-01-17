!/bin/bash



# Download anaconda in /mnt directory where there is more storage space

sudo wget http://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh -P /mnt/



# install anaconda

bash /mnt/Anaconda*.sh



# source bashrc

. ~/.bashrc



# install python 2.7

conda install python=2.7



# create security certs

mkdir certs

sudo openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout certs/private_key.pem -out certs/private_key.pem



# generate a config file

jupyter notebook --generate-config





# modify config file to allow jupyter to run as a server

echo "c = get_config()" >> ~/.jupyter/jupyter_notebook_config.py

echo "c.NotebookApp.ip='*'" >> ~/.jupyter/jupyter_notebook_config.py

echo "c.NotebookApp.open_browser=False" >> ~/.jupyter/jupyter_notebook_config.py

echo "c.NotebookApp.port=8888" >> ~/.jupyter/jupyter_notebook_config.py

echo "c.NotebookApp.certs='/home/hadoop/certs'" >> ~/.jupyter/jupyter_notebook_config.py



# modify bashrc to run pyspark as python notebook

echo "export PYSPARK_DRIVER_PYTHON=jupyter" >> ~/.bashrc

echo "export PYSPARK_DRIVER_PYTHON_OPTS='notebook'" >> ~/.bashrc



# source bashrc

. ~/.bashrc