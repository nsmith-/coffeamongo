## Install dependencies
Conda:
```
conda env create -f environment.yaml
conda activate mongo
```
venv:
```
python3 -m venv mongo36
source mongo36/bin/activate
pip install --upgrade pip setuptools
pip install -r requirements.txt
```

## Start a server
```
mongod --dbpath data --bind_ip 127.0.0.1
```

## Start a client, make users
```
mongo
> use admin
> db.createUser({user:'coffea', pwd: 'password', roles:[{db:'coffeadb', role:'readWrite'}]})
> db.auth('coffea', 'password')
```
