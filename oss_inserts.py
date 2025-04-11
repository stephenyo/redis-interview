import redis
import random

# oss config
REDIS_OSS_HOST = '34.138.248.158'
REDIS_OSS_PORT = 10001
REDIS_OSS_DB = 0
redis_oss = redis.StrictRedis(host=REDIS_OSS_HOST, port=REDIS_OSS_PORT, db=REDIS_OSS_DB)

# ent config
REDIS_ENTERPRISE_HOST = '35.231.226.95'
REDIS_ENTERPRISE_PORT = 18070
REDIS_ENTERPRISE_DB = 0
redis_enterprise = redis.StrictRedis(host=REDIS_ENTERPRISE_HOST, port=REDIS_ENTERPRISE_PORT, db=REDIS_ENTERPRISE_DB, decode_responses=True)


def insert_ordered_data_oss():
    """Inserts values 1-100 into Redis OSS using RPUSH."""
    print("Inserting values 1-100 into Redis OSS...")
    for i in range(1, 101):
        redis_oss.rpush('ordered_numbers', i)  # rpush
    print("Ordered numbers inserted into Redis OSS.")

def insert_random_data_oss():
    """Inserts 100 random values into Redis OSS using a Sorted Set."""
    print("\nInserting 100 random values into Redis OSS...")
    random_values = [random.randint(1, 1000) for _ in range(100)]
    redis_oss.zadd('random_numbers', {str(val): val for val in random_values})
    print("Random numbers inserted into Redis OSS.")

def print_ordered_reversed_enterprise():
    """Prints values 1-100 in reverse order from Redis Enterprise."""
    print("\nPrinting values 1-100 in reverse order from Redis Enterprise...")
    try:
        ordered_data = redis_enterprise.lrange('ordered_numbers', 0, 99)
        if ordered_data:
            for num in ordered_data[::-1]:  
                print(int(num))
        else:
            print("Key 'ordered_numbers' not found in Redis Enterprise.")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis Enterprise: {e}")

def print_random_descending_enterprise():
    """Prints 100 random values in descending order from Redis Enterprise."""
    print("\nPrinting 100 random values in descending order from Redis Enterprise...")
    try:
        descending_random = redis_enterprise.zrevrange('random_numbers', 0, -1)
        if descending_random:
            for num_str in descending_random:
                print(int(num_str))
        else:
            print("Key 'random_numbers' not found in Redis Enterprise.")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis Enterprise: {e}")


def clear_all_data():
    """Clears the 'ordered_numbers' and 'random_numbers' keys from both Redis OSS and Enterprise."""
    print("\nClearing data from Redis OSS...")
    redis_oss.delete('ordered_numbers', 'random_numbers')
    print("Clearing data from Redis Enterprise...")
    redis_enterprise.delete('ordered_numbers', 'random_numbers')
    print("Data cleared from both databases.")


def list_data():
    """Lists the contents of 'ordered_numbers' and 'random_numbers' from Redis Enterprise."""
    print("\n--- Ordered Numbers from Redis Enterprise ---")
    try:
        ordered_data = redis_enterprise.lrange('ordered_numbers', 0, -1)
        if ordered_data:
            print([int(x) for x in ordered_data])
        else:
            print("Key 'ordered_numbers' is empty or not found.")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis Enterprise: {e}")

    print("\n--- Random Numbers from Redis Enterprise ---")
    try:
        random_data = redis_enterprise.zrange('random_numbers', 0, -1, withscores=True)
        if random_data:
            print([(int(x[0]), x[1]) for x in random_data])
        else:
            print("Key 'random_numbers' is empty or not found.")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis Enterprise: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        command = sys.argv[1]
        if command == "insert_ordered":
            insert_ordered_data_oss()
            print("done. now run insert_random.")
        elif command == "insert_random":
            insert_random_data_oss()
            print("done.")
        elif command == "print":
            print_ordered_reversed_enterprise()
            print_random_descending_enterprise()
        elif command == "clear":
            clear_all_data()
        elif command == "list":
            list_data()
        else:
            print("Usage: python data_script.py <command>")
            print("Commands: insert_ordered, insert_random, print, clear, list")
    else:
        print("Usage: python data_script.py <command>")
        print("Commands: insert_ordered, insert_random, print, clear, list")