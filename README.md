# dynamo-storage
A server for Dynamo data storage, management and processing.

### How to run with Docker

```
From the terminal, enter the folder where you want to keep the project and perform the following steps:

$ git clone https://github.com/OpenFairWind/dynamo-storage

$ cd dynamo-storage

$ cd keys

$ openssl genrsa -out dynamo-store-private.pem 2048

$ openssl rsa -in dynamo-store-private.pem -outform PEM -pubout -out dynamo-store-public.pem

$ mkdir public

$ cd public

copy client public key here

go to main directory

$ docker build . -t dynamo-storage

$ docker-compose up -d
```