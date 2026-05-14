import os
import random
import copy
import json
import time
from redis import Redis
from redis.lock import Lock as RedisLock
from multiprocessing import Process

import re
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import LockTimeout

def main(process_transaction, run_redis_comparison_with_ground_truth, cleanup_redis_database, generate_all_transactions, run_transactions_processor_simple_lock, run_transactions_processor_fencing_lock, bcolors):
    print("Generating transactions data ...")
    all_transactions = generate_all_transactions(n_users=50, n_max_transactions=230)

    # Generate the true state of the users to be compared with each one.
    all_users_purchases_state = {}
    for transaction in all_transactions:
        all_users_purchases_state = process_transaction(transaction, all_users_purchases_state)
    
    max_number_of_workers = 110

    # Separate the transactions into batches.
    if len(all_transactions) % max_number_of_workers == 0:
        BATCH_SIZE = (len(all_transactions) // max_number_of_workers)
    else:
        BATCH_SIZE = (len(all_transactions) // max_number_of_workers) + 1
        
    transactions_batches = [
        all_transactions[i : i + BATCH_SIZE]
        for i in range(0, len(all_transactions), BATCH_SIZE)
    ]
    
    for simulated_garbage_collector_pause in [True, False]:        
        # Exercise 1 - Using Redislock   
        print(bcolors.OKGREEN)
        print(f"Exercise 1 - Using Redislock (Simulated Garbage Collector: {simulated_garbage_collector_pause})")
        print(f"Number of Workers: {len(transactions_batches)}.")            
        cleanup_redis_database(host="localhost", port=6379)

        redislock_start = time.time()
        procs = []
        for batch_id in range(len(transactions_batches)):    
            worker_name = f"worker_{batch_id}"
            transactions_to_process = transactions_batches[batch_id]
            #run_transactions_processor_simple_lock
            proc = Process(target=run_transactions_processor_simple_lock, args=(worker_name,transactions_to_process, simulated_garbage_collector_pause))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()
        redislock_end = time.time()

        print(f"Elapsed wall time for: {redislock_end - redislock_start}")
        print("Checking the consistency of Redislock with unpredictable timeouts:")
        run_redis_comparison_with_ground_truth(all_users_purchases_state)
        print(bcolors.ENDC)
        
        # Skip Exercise 2 until we fix the ZooKeeper issue.
        #continue
        # Exercise 2 - Using Zookeeper Fencing Lock
        print(bcolors.OKCYAN)
        print(f"Exercise 2 - Using Zookeeper (Simulated Garbage Collector: {simulated_garbage_collector_pause})")   
        print(f"Number of Workers: {len(transactions_batches)}.")            
        cleanup_redis_database(host="localhost", port=6379)
    
        zookeeper_start = time.time()
        procs = []
        for batch_id in range(len(transactions_batches)):    
            worker_name = f"worker_{batch_id}"
            transactions_to_process = transactions_batches[batch_id]
            #run_transactions_processor_simple_lock
            proc = Process(target=run_transactions_processor_fencing_lock, args=(worker_name,transactions_to_process, simulated_garbage_collector_pause))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()
        zookeeper_end = time.time()

        print(f"Elapsed wall time for: {zookeeper_end - zookeeper_start}")
        print("Checking the consistency of Zookeeper Locker with unpredictable timeouts:")
        run_redis_comparison_with_ground_truth(all_users_purchases_state)
        print(bcolors.ENDC)


"""Updates the purchase state of a player."""
def process_transaction(transaction, users_purchases_state):
    # Make a deep copy so mutable state cannot be blamed for inconsistencies.
    transaction = copy.deepcopy(transaction)
    users_purchases_state = copy.deepcopy(users_purchases_state)

    user_id = transaction["user_id"]
    transaction_id = transaction["transaction_id"]
    price = transaction["price"]    
    # Initialize state if it does not exist.
    if user_id not in users_purchases_state:
        users_purchases_state[user_id] = {"user_id": user_id, "gross_revenue": 0, "purchases": 0, "transactions": []}
    
    # Only update the player state if the transaction if it has not been processed yet.
    if transaction_id not in users_purchases_state[user_id]["transactions"]:
        users_purchases_state[user_id]["gross_revenue"] += price
        users_purchases_state[user_id]["purchases"] += 1
        users_purchases_state[user_id]["transactions"] = sorted(users_purchases_state[user_id]["transactions"] + [transaction_id])
        
    return users_purchases_state

def simulate_unpreditable_garbage_collector_pause(random_generator, simulated_garbage_collector_pause):
    probability_of_pause = 0.5/100
    avg_pause_in_seconds = 400/1000

    if simulated_garbage_collector_pause == True:
        if random_generator.uniform(0, 1) <= probability_of_pause:
            # Garbage collector will pause.
            exponential_distribution_lambda = 1/avg_pause_in_seconds

            time.sleep(random_generator.expovariate(exponential_distribution_lambda))

def run_redis_comparison_with_ground_truth(all_users_purchases_state):
    # Compare the ground truth with the result of the concurrent processing.
    connection = Redis(host="localhost", port=6379, decode_responses=True)
    results_divergence = False
    for user_id in all_users_purchases_state:
        true_gross_revenue = all_users_purchases_state[user_id]["gross_revenue"]
        
        redis_user_state = json.loads(connection.get(user_id) or "{}")
        redis_gross_revenue = None
        if user_id in redis_user_state:
            redis_gross_revenue = redis_user_state[user_id].get("gross_revenue", 0)
        
        if (true_gross_revenue != redis_gross_revenue):
            results_divergence = True
            computation_error = (true_gross_revenue - redis_gross_revenue)/true_gross_revenue
            print(f"User {user_id} has an inconsistent state. True: {true_gross_revenue} != Redis: {redis_gross_revenue}. Percentual Error: {round(100*computation_error, 2)}%.")
            
    if results_divergence == False:
        print("The results are consistent.")        
        
    connection.close()

def cleanup_redis_database(host, port):
    connection = Redis(host=host, port=port, decode_responses=True)
    connection.flushdb()
    connection.close()

### Ground Truth
"""Generate all transactions"""
def generate_all_transactions(n_users, n_max_transactions):
    data_generation_random = random.Random()
    data_generation_random.seed(345)

    all_transactions = []
    users = range(1, n_users + 1)
    for user_number in users:
        user_number = str(user_number).zfill(5)
        
        transactions = range(1, data_generation_random.randint(1, 500 + n_max_transactions))
        for transaction_number in transactions:
            transaction_number = str(transaction_number).zfill(5)
            all_transactions.append({"user_id": f"my_game_id:my_user_id_{user_number}",
                                    "transaction_id": f"my_game_id:my_user_id_{user_number}:{transaction_number}",
                                    "price": data_generation_random.randint(1, 100)
                                    })

    # We don't guarantee that the transactions are in order.            
    data_generation_random.shuffle(all_transactions)
    return all_transactions

### Example 2: Processing transactions concurrently and subject to Garbage Collector pauses.
def run_transactions_processor_simple_lock(processor_name, transactions_to_process, simulated_garbage_collector_pause):
    # Make a deep copy so mutable state cannot be blamed for inconsistencies.    
    transactions_to_process = copy.deepcopy(transactions_to_process)
    # Define private random generator.
    random_generator = random.Random()
    random_generator.seed(processor_name)
    
    # Instantiate Redis.
    redis_connection = Redis(host="localhost", port=6379, decode_responses=True)
    locker_connection = Redis(host="localhost", port=6379, decode_responses=True)
    
    for transaction in transactions_to_process:
        user_id = transaction["user_id"]
        transaction_id = transaction["transaction_id"]
    
        # Generate a per-user random token in order to check protect ourselves 
        # from operations that took longer than expected. Would that work?
        while True:
            # We will use Redis for both: Locking and Data Storage
            exclusive_lock_token = os.urandom(32).hex()

            # Try to acquire the lock. If we fail, we try again.
            # DON'T USE THIS IN PRODUCTION. Trying stopping retrying to acquire a lock 
            # when you don't expect to have a lot of contention is a **very bad idea**.
            # At least you should wait a random amount of time before retrying in order
            # to decouple the requests of all workers.
            lock_name = f"__purchase_state_lock__:{user_id}"
            user_lock = RedisLock(redis=locker_connection, name=lock_name, timeout=0.2, blocking=False)
            
            if user_lock.acquire(blocking=True, blocking_timeout=0.2) == True:
                user_purchase_state = json.loads(redis_connection.get(user_id) or '{}')
                current_exclusive_lock_token = user_purchase_state.get("exclusive_lock_token", None)

                # Update the state and the exclusive lock.
                user_purchase_state = process_transaction(transaction, user_purchase_state)
                user_purchase_state["exclusive_lock_token"] = exclusive_lock_token
                
                
                # Before updating the state, check if the lock token hasn't changed.
                # Maybe the process_transaction took too long.
                reread_user_state = json.loads(redis_connection.get(user_id) or "{}")
                reread_exclusive_lock_token = reread_user_state.get("exclusive_lock_token", None)
                if reread_exclusive_lock_token == current_exclusive_lock_token:
                    # Add a random pause to simulate an unpredictable garbage collector pause.
                    simulate_unpreditable_garbage_collector_pause(random_generator=random_generator, simulated_garbage_collector_pause=simulated_garbage_collector_pause)
                        
                    redis_connection.set(user_id, json.dumps(user_purchase_state))
                else:
                    print(f"[Woker: {processor_name}]: Something changed before we could update the state of {user_id} for {transaction_id}. Retrying.")

                try:                    
                    # Try to release the lock.
                    user_lock.release()
                except:
                    pass
                
                # Process next transaction
                break
    locker_connection.close()            
    redis_connection.close()
    



def run_transactions_processor_fencing_lock(processor_name, transactions_to_process, simulated_garbage_collector_pause):
    # Make a deep copy so mutable state cannot be blamed for inconsistencies.    
    transactions_to_process = copy.deepcopy(transactions_to_process)
    # Define private random generator.
    random_generator = random.Random()
    random_generator.seed(processor_name)
    
    # Instantiate Redis.
    def my_abstract_listener(locker_connection, state):
        # Force error.
        # Warning: We should signal with an error so code should be aware of the problem.
        if state != KazooState.CONNECTED:
            raise Exception("For some reason I was disconnected from ZooKeeper. I will stop now.")

    redis_connection = Redis(host="localhost", port=6379, decode_responses=True)
    locker_connection = KazooClient(hosts='127.0.0.1:2181')
    
    my_concrete_listener = lambda state: my_abstract_listener(locker_connection, state)
    while True:
        locker_connection.start()
        if locker_connection.connected:
            locker_connection.add_listener(my_concrete_listener)
            break
    
    for transaction in transactions_to_process:
        user_id = transaction["user_id"]
    
        while True:
            # A new Lock object is created per attempt because kazoo's Lock does not support re-acquisition.
            # ZooKeeper's sequential counter for a given path is 32-bit (~2.1 billion), which is not
            # a practical concern at normal lock acquisition rates.
            user_lock = locker_connection.Lock(f"/app/purchases_processor/users/state_lock/{user_id}", processor_name)
            
            lock_acquired = False
            try:
                lock_acquired = user_lock.acquire(blocking=True, timeout=0.2, ephemeral=True)
            except LockTimeout:
                continue
                
            if lock_acquired == True:
                # ZooKeeper will add to the lock node a monotonically increasing number.
                # Let's use it as our fencing token.
                monotonic_lock_id = re.search(r"__lock__(\d+)", user_lock.node).group(1)
                
                user_purchase_state = json.loads(redis_connection.get(user_id) or '{}')
                user_purchase_state = process_transaction(transaction, user_purchase_state)

                # Add a random pause to simulate an unpredictable garbage collector pause.
                simulate_unpreditable_garbage_collector_pause(random_generator=random_generator, simulated_garbage_collector_pause=simulated_garbage_collector_pause)
                
                # Submit the update to the database using fencing token inside a serializability.
                fencing_token_logic_script = '''
                    -- Implement fencing token logic using Lua Script on Redis.
                    -- We use Lua scripts because they are executed atomically.

                    -- Two keys per user: one for the state, one for the fencing token.
                    -- Keeping them separate avoids deserializing the full state blob just to compare tokens.

                    -- tonumber() is safe here: ZooKeeper's counter is 32-bit (max ~2.1B), which fits
                    -- exactly in a Lua number (IEEE 754 double, exact integers up to 2^53).
                    local current_fencing_token = tonumber(redis.call("get", KEYS[2]))
                    local new_fencing_token_candidate = tonumber(ARGV[2])

                    if current_fencing_token == nil or current_fencing_token < new_fencing_token_candidate then
                        -- Update the fencing token and the value.
                        redis.call("set", KEYS[1], ARGV[1])    
                        redis.call("set", KEYS[2], ARGV[2])    
                        return 1
                    else
                        -- The fencing token is outdated.
                        return 0
                    end                
                '''
                
                user_state_content = json.dumps(user_purchase_state)
                fencing_token_key = f"__fencing_token:{user_id}"
                update_status = redis_connection.eval(fencing_token_logic_script,
                                      2,
                                      user_id, 
                                      fencing_token_key,                                      
                                      user_state_content,
                                      monotonic_lock_id,
                                      )
                
                user_lock.release()
                
                if update_status == 1:
                    # Successful change in the database. 
                    # Process next
                    break
        
    locker_connection.remove_listener(my_concrete_listener)    
    locker_connection.stop()
    locker_connection.close()
    redis_connection.close()

# Terminal colors definitions
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

if __name__ == "__main__":
    main(process_transaction, run_redis_comparison_with_ground_truth, cleanup_redis_database, generate_all_transactions, run_transactions_processor_simple_lock, run_transactions_processor_fencing_lock, bcolors)
