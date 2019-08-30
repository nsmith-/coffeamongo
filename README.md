Install dependencies
```
conda env create -f environment.yaml
conda activate mongo
```

Start a server
```
mongod --dbpath data --bind_ip 127.0.0.1
```

Start a client, make users
```
mongo
> use admin
> db.createUser({user:'coffea', pwd: 'password', roles:[{db:'coffeadb', role:'readWrite'}]})
> db.auth('coffea', 'password')
```
