import redis
import sys

# Configuration for Redis OSS (Writes)
REDIS_OSS_HOST = '34.138.248.158'  
REDIS_OSS_PORT = 10001  # Configured to run on port 10001
REDIS_OSS_DB = 0
redis_oss = redis.StrictRedis(host=REDIS_OSS_HOST, port=REDIS_OSS_PORT, db=REDIS_OSS_DB, decode_responses=True)

# Configuration for Redis Enterprise (Reads)
REDIS_ENTERPRISE_HOST = '35.231.226.95'  
REDIS_ENTERPRISE_PORT = 18070  # port from entdb UI
REDIS_ENTERPRISE_DB = 0
redis_enterprise = redis.StrictRedis(host=REDIS_ENTERPRISE_HOST, port=REDIS_ENTERPRISE_PORT, db=REDIS_ENTERPRISE_DB, decode_responses=True)


def create_user(user_id, name):
    """Creates a user (in OSS)."""
    user_key = f'user:{user_id}'
    redis_oss.hset(user_key, mapping={'name': name})  # Write to OSS using hashes
    print(f"User '{name}' (ID: {user_id}) created in OSS.")


def create_product(sku, name, category, price):
    """Creates a product (in OSS)."""
    product_key = f'product:{sku}'
    redis_oss.hset(product_key, mapping={'name': name, 'category': category, 'price': price})  # Write to OSS using hashes
    print(f"Product '{name}' (SKU: {sku}) created in OSS.")


def get_user_id_by_name(name):
    """Retrieves a user ID by name (from Enterprise)."""
    for key in redis_enterprise.scan_iter("user:*"):  # Read users from Enterprise db
        if redis_enterprise.hget(key, 'name') == name:
            return key.split(":")[1]
    return None


def add_to_cart(user_id, sku, quantity=1):
    """Adds an item to a user's shopping cart (in OSS) using user_id."""
    cart_key = f'cart:{user_id}'
    redis_oss.hincrby(cart_key, sku, quantity)  # Write to OSS using HINCRBY
    print(f"{quantity} of SKU {sku} added to cart for User ID: {user_id} in OSS.")

def remove_from_cart(user_id, sku, quantity=1):
    """Removes an item from a user's shopping cart (in OSS) using user_id."""
    cart_key = f'cart:{user_id}'
    current_quantity = redis_oss.hget(cart_key, sku)  
    if current_quantity:
        new_quantity = int(current_quantity) - quantity
        if new_quantity > 0:
            redis_oss.hset(cart_key, sku, new_quantity)  # Write to OSS
            print(f"Removed {quantity} of SKU {sku} from cart for User ID: {user_id} in OSS. New quantity: {new_quantity}")
        elif new_quantity == 0:
            redis_oss.hdel(cart_key, sku)  # Write to OSS
            print(f"Removed all {sku}(s) from cart for User ID: {user_id} in OSS.")
        else:
            print(f"Cannot remove {quantity} {sku}(s) as only {current_quantity} exists in the cart.")
    else:
        print(f"{sku} not found in cart for User ID: {user_id}.")

def show_cart(user_id):
    """Shows the shopping cart for a specific user (from Enterprise) using user_id."""
    cart_key = f'cart:{user_id}'
    cart_items = redis_enterprise.hgetall(cart_key)  # Read from Enterprise using hgetall
    print(f"\n--- Cart for User ID: {user_id} ---")
    if cart_items:
        total = 0.0
        for sku, quantity in cart_items.items():
            product_key = f'product:{sku}'
            product_data = redis_enterprise.hgetall(product_key)  
            if product_data:
                price = float(product_data.get('price'))
                item_total = price * int(quantity)
                total += item_total
                print(f"  - {product_data.get('name')} ({sku}): {quantity} x ${price:.2f} = ${item_total:.2f}")
            else:
                print(f"  - {sku}: {quantity} (Product details not found)")

        tax_rate = 0.0725
        tax = total * tax_rate
        final_total = total + tax
        print(f"  Subtotal: ${total:.2f}")
        print(f"  Sales Tax (7.25%): ${tax:.2f}")
        print(f"  Total: ${final_total:.2f}")
    else:
        print("  Cart is empty.")

def show_all_carts():
    """Shows all existing shopping carts (from Enterprise)."""
    cart_keys = redis_enterprise.keys('cart:*')  # Read from Enterprise
    if not cart_keys:
        print("No shopping carts exist.")
        return

    print("\n--- All Carts ---")
    for cart_key in cart_keys:
        user_id = cart_key.split(':')[1]
        user_name = redis_enterprise.hget(f'user:{user_id}', 'name')  
        cart_items = redis_enterprise.hgetall(cart_key)  
        print(f"Cart for {user_name} (ID: {user_id}):")
        if cart_items:
            total = 0.0
            for sku, quantity in cart_items.items():
                product_key = f'product:{sku}'
                product_data = redis_enterprise.hgetall(product_key)  
                if product_data:
                    price = float(product_data.get('price'))
                    item_total = price * int(quantity)
                    total += item_total
                    print(f"    - {product_data.get('name')} ({sku}): {quantity} x ${price:.2f}")
                else:
                    print(f"    - {sku}: {quantity} (Product details not found)")
            print(f"    Total items: {sum(int(qty) for qty in cart_items.values())}")
        else:
            print("    Cart is empty.")


def list_users():
    """Lists all users (from Enterprise)."""
    user_keys = redis_enterprise.keys('user:*')  # Read from Enterprise
    if not user_keys:
        print("No users found.")
        return

    print("\n--- Users ---")
    for user_key in user_keys:
        user_id = user_key.split(':')[1]
        user_data = redis_enterprise.hgetall(user_key)  # Read from Enterprise
        if user_data:
            print(f"  - User ID: {user_id}, Name: {user_data.get('name')}")
        else:
            print(f"  - User ID: {user_id}, User data not found.")


def list_products(category=None):
    """Lists all products (from Enterprise), optionally filtered by category."""
    product_keys = redis_enterprise.keys('product:*')  # Read from Enterprise
    if not product_keys:
        print("No products found.")
        return

    print("\n--- Products ---")
    for product_key in product_keys:
        product_data = redis_enterprise.hgetall(product_key)  # Read from Enterprise
        if product_data:
            if category:
                if product_data.get('category') == category:
                    print(f"  - SKU: {product_key.split(':')[1]}, Name: {product_data.get('name')}, Price: {product_data.get('price')}, Category: {product_data.get('category')}")
            else:
                print(f"  - SKU: {product_key.split(':')[1]}, Name: {product_data.get('name')}, Price: {product_data.get('price')}, Category: {product_data.get('category')}")
        else:
            print(f"  - SKU: {product_key.split(':')[1]}, Product data not found.")


def clear_all_data():
    """Clears all data (users and carts) from Redis OSS and products from Redis Enterprise."""
    # Clear users and carts from OSS
    user_keys_oss = redis_oss.keys('user:*')
    cart_keys_oss = redis_oss.keys('cart:*')
    if user_keys_oss:
        redis_oss.delete(*user_keys_oss)
        print(f"\nCleared {len(user_keys_oss)} users from OSS.")
    if cart_keys_oss:
        redis_oss.delete(*cart_keys_oss)
        print(f"Cleared {len(cart_keys_oss)} carts from OSS.")

    # Clear products from Enterprise
    product_keys_enterprise = redis_enterprise.keys('product:*')
    if product_keys_enterprise:
        redis_enterprise.delete(*product_keys_enterprise)
        print(f"Cleared {len(product_keys_enterprise)} products from Enterprise.")


def delete_user(user_id):
    """Deletes a user (from OSS) by user ID."""
    user_key = f'user:{user_id}'
    cart_key = f'cart:{user_id}'

    # Check if the user exists before deleting (optional but recommended)
    if redis_oss.exists(user_key):
        redis_oss.delete(user_key, cart_key)  # Delete user and their cart
        print(f"User with ID {user_id} and their cart deleted from OSS.")
    else:
        print(f"User with ID {user_id} not found in OSS. Deletion skipped.")


def clear_all_carts_oss():
    """Clears all shopping carts from Redis OSS."""
    cart_keys_oss = redis_oss.keys('cart:*')  # Get all cart keys in OSS
    if cart_keys_oss:
        redis_oss.delete(*cart_keys_oss)  # Delete all cart keys
        print(f"\nCleared {len(cart_keys_oss)} shopping cart(s) from OSS.")
    else:
        print("No shopping carts found in OSS to clear.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python oss_shopping.py <command> [options]")
        print("Commands: create_user, create_product, add_to_cart, remove_from_cart, show_cart, show_all_carts, list_users, list_products, clear_all_data, delete_user, clear_all_carts_oss")
    else:
        command = sys.argv[1]
        if command == "create_user":
            if len(sys.argv) == 4:
                user_id = sys.argv[2]
                name = sys.argv[3]
                create_user(user_id, name)
            else:
                print("Usage: python oss_shopping.py create_user <user_id> <name>")

        elif command == "create_product":
            if len(sys.argv) == 6:
                sku = sys.argv[2]
                name = sys.argv[3]
                category = sys.argv[4]
                price = float(sys.argv[5])
                create_product(sku, name, category, price)
            else:
                print("Usage: python oss_shopping.py create_product <sku> <name> <category> <price>")

        elif command == "add_to_cart":
            if len(sys.argv) >= 4:
                user_id = sys.argv[2] 
                sku = sys.argv[3]
                quantity = int(sys.argv[4]) if len(sys.argv) > 4 else 1
                add_to_cart(user_id, sku, quantity)
            else:
                print("Usage: python oss_shopping.py add_to_cart <user_id> <sku> [quantity]")

        elif command == "remove_from_cart":
            if len(sys.argv) >= 4:
                user_id = sys.argv[2]  
                sku = sys.argv[3]
                quantity = int(sys.argv[4]) if len(sys.argv) > 4 else 1
                remove_from_cart(user_id, sku, quantity)
            else:
                print("Usage: python oss_shopping.py remove_from_cart <user_id> <sku> [quantity]")

        elif command == "show_cart":
            if len(sys.argv) == 3:
                user_id = sys.argv[2]  
                show_cart(user_id)
            else:
                print("Usage: python oss_shopping.py show_cart <user_id>")

        elif command == "show_all_carts":
            show_all_carts()

        elif command == "list_users":
            list_users()

        elif command == "list_products":
            if len(sys.argv) == 3:
                category = sys.argv[2]
                list_products(category)
            else:
                list_products()

        elif command == "clear_all_data":
            clear_all_data()

        elif command == "delete_user":
            if len(sys.argv) == 3:
                user_id = sys.argv[2]
                delete_user(user_id)
            else:
                print("Usage: python oss_shopping.py delete_user <user_id>")

        elif command == "clear_all_carts_oss":
            clear_all_carts_oss()

        else:
            print(f"Unknown command: {command}")
            print("Usage: python oss_shopping.py <command> [options]")
            print("Commands: create_user, create_product, add_to_cart, remove_from_cart, show_cart, show_all_carts, list_users, list_products, clear_all_data, delete_user, clear_all_carts_oss")
