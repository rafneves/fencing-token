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

## The Window of Vulnerability

Any delay between acquiring the lock and completing the write is a window for corruption — regardless of cause:

- **Process suspension** — GC pause, OS scheduling jitter, VM or container suspension
- **Slow or retried write to Redis** — the lock expires while the write is still in flight
- **Redis connection timeout** — the worker retries a write that already landed, or lands it after another worker committed newer state
- **Network partition to ZooKeeper** — the lock appears held but the session is already gone

A GC pause is just the most concrete and reproducible example to demo. The fencing token handles all of these cases by reinstating **total ordering** on writes — any write carrying a token older than the last accepted one is provably out of order and rejected, regardless of why it arrived late.

## How GC Pauses Are Simulated

Implemented in `simulate_unpreditable_garbage_collector_pause`. Each transaction has a **0.5% chance** of triggering a `time.sleep` injected *after* the lock is acquired and *before* the write — the exact window described above. The duration is drawn from an **exponential distribution** (mean 400ms) to mimic the unpredictability of real stop-the-world events.

## Sample Output

**With GC pauses enabled**, RedisLock silently loses data for 30 out of 50 users — errors ranging from under 1% to over 24%. ZooKeeper is consistent, at the cost of ~2.4× the wall time.

**Without GC pauses**, the picture improves but RedisLock *still* produces 3 inconsistencies from normal lock contention alone. ZooKeeper remains consistent.

```
Exercise 1 - Using Redislock (Simulated Garbage Collector: True)
Number of Workers: 110.
[Worker: worker_25]: Something changed before we could update the state of my_game_id:my_user_id_00024 for my_game_id:my_user_id_00024:00189. Retrying.
[Worker: worker_47]: Something changed before we could update the state of my_game_id:my_user_id_00041 for my_game_id:my_user_id_00041:00449. Retrying.
[Worker: worker_37]: Something changed before we could update the state of my_game_id:my_user_id_00024 for my_game_id:my_user_id_00024:00562. Retrying.
[Worker: worker_43]: Something changed before we could update the state of my_game_id:my_user_id_00030 for my_game_id:my_user_id_00030:00084. Retrying.
Elapsed wall time for: 20.455346822738647
Checking the consistency of Redislock with unpredictable timeouts:
User my_game_id:my_user_id_00016 has an inconsistent state. True: 24612 != Redis: 23835. Percentual Error: 3.16%.
User my_game_id:my_user_id_00005 has an inconsistent state. True: 22195 != Redis: 20598. Percentual Error: 7.2%.
User my_game_id:my_user_id_00035 has an inconsistent state. True: 19449 != Redis: 18758. Percentual Error: 3.55%.
User my_game_id:my_user_id_00028 has an inconsistent state. True: 33652 != Redis: 32511. Percentual Error: 3.39%.
[... snip... ]
User my_game_id:my_user_id_00048 has an inconsistent state. True: 21140 != Redis: 19561. Percentual Error: 7.47%.
User my_game_id:my_user_id_00008 has an inconsistent state. True: 19286 != Redis: 18941. Percentual Error: 1.79%.
User my_game_id:my_user_id_00046 has an inconsistent state. True: 15494 != Redis: 15154. Percentual Error: 2.19%.
User my_game_id:my_user_id_00021 has an inconsistent state. True: 16634 != Redis: 15377. Percentual Error: 7.56%.

Exercise 2 - Using Zookeeper (Simulated Garbage Collector: True)
Number of Workers: 110.
Elapsed wall time for: 48.89732885360718
Checking the consistency of Zookeeper Locker with unpredictable timeouts:
The results are consistent.


Exercise 1 - Using Redislock (Simulated Garbage Collector: False)
Number of Workers: 110.
[Worker: worker_3]: Something changed before we could update the state of my_game_id:my_user_id_00028 for my_game_id:my_user_id_00028:00235. Retrying.
[Worker: worker_20]: Something changed before we could update the state of my_game_id:my_user_id_00048 for my_game_id:my_user_id_00048:00401. Retrying.
Elapsed wall time for: 18.47313380241394
Checking the consistency of Redislock with unpredictable timeouts:
User my_game_id:my_user_id_00028 has an inconsistent state. True: 33652 != Redis: 33614. Percentual Error: 0.11%.
User my_game_id:my_user_id_00039 has an inconsistent state. True: 22435 != Redis: 22344. Percentual Error: 0.41%.
User my_game_id:my_user_id_00048 has an inconsistent state. True: 21140 != Redis: 21133. Percentual Error: 0.03%.

Exercise 2 - Using Zookeeper (Simulated Garbage Collector: False)
Number of Workers: 110.
Elapsed wall time for: 45.27469205856323
Checking the consistency of Zookeeper Locker with unpredictable timeouts:
The results are consistent.
```

> **Note on "Retrying" lines:** the re-read token check catches some races, but not all — a GC pause after the check silently lets a stale write through with no retry and no error. The GC=False run shows the same: even without artificial pauses, normal lock expiry under contention is enough to lose 3 writes.

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
