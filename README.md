# Fencing Token Demo: RedisLock vs ZooKeeper

A hands-on demo built while reading Martin Kleppmann's post [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) and Chapter 8 of [Designing Data-Intensive Applications](https://dataintensive.net/). The goal was to actually *see* the problem happen, not just understand it theoretically.

## What This Shows

The central trade-off in distributed locking: **speed vs. correctness under failure**.

Two approaches to protecting a shared resource (a Redis key-value store) under concurrent writes from 110 workers, with simulated garbage collector pauses that cause processes to hold a lock longer than expected:

1. **RedisLock (naïve)** — fast and optimistic. Works well under low contention, but a GC pause after acquiring the lock and before writing can cause two workers to believe they hold the lock simultaneously, silently producing inconsistent state.

2. **ZooKeeper + Fencing Token** — slower, but correct. ZooKeeper's writes are linearizable (via the ZAB consensus protocol), so the monotonically increasing token it issues with every lock acquisition is trustworthy. The write to Redis is guarded by a Lua script that rejects any write carrying a token older than the last accepted one. A stale writer is *rejected at the storage layer*, not just slowed down — the system stays correct even when a process comes back from a long pause.

The script generates synthetic purchase transactions for a set of users, then processes them sequentially in a single thread to produce the mathematically correct state per user — the ground truth. Only after that does concurrency start. When all workers finish, it reads what each strategy left in the database and compares `gross_revenue` per user against that pre-computed value. Any divergence is a lost or overwritten write, reported as a percentage error.

## How the Fencing Token Works

When a worker acquires the ZooKeeper lock for a user, ZooKeeper creates an ephemeral sequential node under the lock path and appends a monotonically increasing number — guaranteed to be totally ordered across all clients via ZAB ([ZooKeeper Atomic Broadcast](https://zookeeper.apache.org/doc/current/zookeeperInternals.html#sc_atomicBroadcast)):

```
/app/purchases_processor/users/state_lock/my_game_id:my_user_id_00001/
    __lock__0000000038   ← previous acquisition
    __lock__0000000042   ← current holder; token = 42
```

The worker extracts `42` as the fencing token and carries it into the write. In Redis, each user has two keys: one for the state and one for the last accepted token:

```
my_game_id:my_user_id_00001            → {"gross_revenue": 1500, "purchases": 12, ...}
__fencing_token:my_game_id:my_user_id_00001  → 42
```

A Lua script on Redis performs the write atomically — comparing the incoming token against the stored one before accepting or rejecting:

```
worker with token 45 → accepted  (45 > 42) → state updated, token advanced to 45
worker with token 38 → rejected  (38 < 45) → write dropped, state untouched
```

The two-key design avoids deserializing the full state blob just to compare tokens. The Lua script guarantees the comparison and the write happen atomically.

## Setup

### Start the services

```sh
docker compose up -d
```

This starts a 3-node ZooKeeper cluster, a [Valkey](https://valkey.io) server, RedisInsight (GUI), and ZooNavigator (ZooKeeper GUI).

> **Valkey?** It is the open source BSD-3 fork of Redis, created in 2024 after Redis changed its license. It is fully protocol-compatible — same commands, same clients, same Lua scripting. You will not notice the difference.

### Install Python dependencies

Requires Python 3.8+. Using a virtual environment is recommended:

```sh
cd code
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run the demo

```sh
python concurrent_writes.py
```

## Inspecting State via GUIs

| Service | URL | Notes |
|---------|-----|-------|
| ZooKeeper Admin | http://localhost:8080/commands/stat | |
| ZooNavigator | http://localhost:9000 | Connection string: `zoo1` · leave auth fields empty |
| RedisInsight | http://localhost:5540 | Add connection manually · Host: `redis-server` · leave auth fields empty |

## Stopping Services

```sh
docker compose down -v
```

## Troubleshooting

### Too many open files

ZooKeeper opens a lot of file descriptors. If you see connection errors on macOS, raise the limit and restart:

```sh
sudo launchctl limit maxfiles 2048 unlimited
```

## License

See [LICENSE](LICENSE).
