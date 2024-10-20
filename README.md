# Example of RedisLock and ZooKeeper fencing token

## Setup

### Launch the services

To launch the services used in this example:
1. Open a Terminal
3. Start services through docker-compose `docker compose up -d`.

This will start ZooKeeper, Redis Server and Redislock and map your computer's local ports `8080` (Zookeeper Admin), `9000` (ZooNavigator) and `5540` (Redis Insights) to the services inside the container.

### Install Python Packages

You will need Python 3.x You can use your own distribution, or install a specific virtual environment:

```sh
cd code
pip3 install venv
python3 -m venv "venv"
source ./venv/bin/activate
pip3 install -r requirements.txt
```

## Testing Redis and Zookeeper through Graphical User Interfaces

To check they are up and running, open your browser and try accessing the following links:

* Zookeeper Admin: http://localhost:8080/commands/stat
* ZooNavigator: http://localhost:9000/
    * Connection string: `zoo1` (the hostname of ZooKeeper 1 machine inside Docker Network)
    * Maintain the empty value for "Auth username" and "Auth Password".
* RedisInsights: http://localhost:5540/
    * Click on "Add connection details manually.
    * Use as `Host` the value `redis-server` (the hostname of Redis Server machine inside Docker Network).

## Stopping Services

You can enter your Docker Desktop and stop the services. If you prefer using the terminal:
To launch the services used in this example:
1. Open a Terminal
2. Go to `chapter08/redis_lock`
3. Stop services through docker-compose `docker compose down`.

## Troubleshooting

### Problems in creating connections

Try to increase `maxfiles`` limits.

```sh
su
launchctl limit maxfiles 2048 unlimited
```
