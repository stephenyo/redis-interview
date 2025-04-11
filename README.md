# Stephen Young - Redis Interview

Here, you will find a few files as well as screenshots for Stephen Young's Redis Interview assignment with Kyle K.

### memtier_keys.txt
Memtier keys inserted to oss db and output from ENT db.

### memtier_replicaOf_output.png
Screenshot showing output from ENTDB replica.

### oss_inserts.py
Python script to insert values 1-100 as well as 100 random values (from 1000) into the oss db.  Also clears data for repeatable demo.

### ordered and random value output.txt
Output of values 1-100 (desc) and descending random values from 1000 (from oss_inserts.py)

### data_model_and_shopping_cart.py
Python app to manage users and carts in the redis db.  Supports user creation, product creation, add/remove to/from cart, clear data, etc.  The app writes to oss and reads from ENT.

### shopping_cart_and_data_model.png
Screenshot of shopping cart data output
