# DYNAMO Signal K storage server

A server for Dynamo data storage, management and processing.

### How to run with Docker

```
From the terminal, enter the folder where you want to keep the project and perform the following steps:

$ git clone https://github.com/OpenFairWind/dynamo-storage

$ cd dynamo-signalk-storage-server

$ cd keys

$ openssl genrsa -out dynamo-signalk-storage-server-private.pem 2048

$ openssl rsa -in dynamo-signalk-storage-server-private.pem -outform PEM -pubout -out dynamo-signalk-storage-server-public.pem

$ mkdir public

$ cd public

copy client public key here

go to main directory

$ docker build . -t dynamo-signalk-storage-server

$ docker-compose up -d
```