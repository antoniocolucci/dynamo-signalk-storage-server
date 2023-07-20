# DYNAMO Signal K storage server

A server for DYNAMO data storage, management and processing.

A reference client implementation as Signal K Server plugin is available here:
https://github.com/OpenFairWind/dynamo-signalk-plugin

## Vessel to shore data movement through the Internet of Floating Things: A microservice platform at the edge

The rise of the Internet of Things has generated high expectations about the improvement in people's lifestyles.
In the last decade, we saw several examples of instrumented cities where different types of data were gathered,
processed, and made available to inspire the next generation of scientists and engineers.

In this framework, sensors and actuators became leading actors of technologically pervasive urban environments.
However, in coastal areas, marine data crowdsourcing is difficult to apply due to the challenging operational
conditions, extremely unstable network connectivity, and security issues in data movement.
To fill this gap, we present a novel version of our DYNAMO transfer protocol (DTP), a platform-independent data mover
framework where data collected on board of vessels are stored locally and then moved from the edge to the cloud when the
operating conditions are favorable.

We evaluate the performance of DTP in a controlled environment with a private cloud by measuring the time it takes for
the clouds ide to process and store a fixed amount of data while varying the number of microservice instances.

We show that the time decreases exponentially when the number of microservice instances goes from 1 to 16, and it
remains constant above that number.

### How to cite:

* Di Luccio, Diana, Sokol Kosta, Aniello Castiglione, Antonio Maratea, and Raffaele Montella.
"Vessel to shore data movement through the internet of floating things: A microservice platform at the edge."
Concurrency and Computation: Practice and Experience 33, no. 4 (2021): e5988.
https://doi.org/10.1002/cpe.5988
* Montella, Raffaele, Diana Di Luccio, Sokol Kosta, Giulio Giunta, and Ian Foster. "Performance, resilience, and
security in moving data from the fog to the cloud: the DYNAMO transfer framework approach." In International
Conference on Internet and Distributed Computing Systems, pp. 197-208. Springer, Cham, 2018.
https://doi.org/10.1007/978-3-030-02738-4_17
* Montella, Raffaele, Mario Ruggieri, and Sokol Kosta. "A fast, secure, reliable, and resilient data transfer framework
for pervasive IoT applications." In IEEE INFOCOM 2018-IEEE Conference on Computer Communications Workshops (INFOCOM
WKSHPS), pp. 710-715. IEEE, 2018.
https://doi.org/10.1109/INFCOMW.2018.8406884
* Montella, Raffaele, Sokol Kosta, and Ian Foster. "DYNAMO: Distributed leisure yacht-carried sensor-network for
atmosphere and marine data crowdsourcing applications." In 2018 IEEE International Conference on Cloud Engineering
(IC2E), pp. 333-339. IEEE, 2018.
https://doi.org/10.1109/IC2E.2018.00064

## Building and run


```git clone https://github.com/OpenFairWind/dynamo-signalk-storage-server.git```

```cd dynamo-signalk-storage-server```

```python3 -m venv venv```

```. venv/bin/activate```

```cp config.json.sample config.json```

```cp docker-compose.yml.development docker-compose```

1st shell:

```cd dynamo-signalk-storage-server```

```docker-compose up```

2st shell (be sure the services are up and running):

```cd dynamo-signalk-storage-server```

```. venv/bin/activate```

```celery --app run worker --loglevel INFO```

3st shell (be sure the worker is up and running):

```cd dynamo-signalk-storage-server```

```. venv/bin/activate```

```flask --app app run -h 0.0.0.0 -p 13387 --debug```


## How to create keys manually

Private key

```openssl genrsa -out dynamo-signalk-storage-server-private.pem 2048```

Public key

```openssl rsa -in dynamo-signalk-storage-server-private.pem -outform PEM -pubout -out dynamo-signalk-storage-server-public.pem```