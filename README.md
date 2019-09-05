## Install dependencies
Conda:
```
conda env create -f environment.yaml
conda activate mongo
```
venv:
```
python3 -m venv --system-site-packages mongo36
source mongo36/bin/activate
pip install --upgrade pip setuptools
pip install -r requirements.txt
```

## Setup server
Start daemon
```
mongod --dbpath data --bind_ip 127.0.0.1
```
Create user, provide necessary roles
```
mongo
> use admin
> db.createUser({user:'coffea', pwd: 'password', roles:[{db:'coffeadb', role:'readWrite'}]})
> db.createRole({"role" : "splitVector", "roles" : [], "privileges" : [{"resource" : {"db" : "coffeadb", "collection" : ""}, "actions" : ["splitVector"]}]})
> db.grantRolesToUser('coffea', [{role:'splitVector', db:'coffeadb'}])
```
