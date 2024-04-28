import psycopg2
from tabulate import tabulate

print("Beginning")

# Change the credentials and the name of the database
con = psycopg2.connect(
    host="localhost",
    port="5433",  # port number
    database="postgres",
    user="postgres",
    password="Harish@5250"
)

print(con)

# For isolation: SERIALIZABLE
con.set_isolation_level(3)
# For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    # QUERY
    # cur.execute("insert into student values (10, 'stud1','usa','A')")
        
    # 1. Deleting product p1 from Product and Stock
    cur.execute("DELETE FROM stock WHERE prodid = 'p1';")
    cur.execute("DELETE FROM product WHERE prodid = 'p1';")

    # 2. Deleting depot d1 from Depot and Stock
    cur.execute("DELETE FROM stock WHERE depid = 'd1';")
    cur.execute("DELETE FROM depot WHERE depid = 'd1';")

    # 3. Changing product p1's name to pp1 in Product and Stock
    cur.execute("UPDATE product SET prodid = 'pp1' WHERE prodid = 'p1';")
    cur.execute("UPDATE stock SET prodid = 'pp1' WHERE prodid = 'p1';")

    # 4. Changing depot d1's name to dd1 in Depot and Stock
    cur.execute("UPDATE depot SET depid = 'dd1' WHERE depid = 'd1';")
    cur.execute("UPDATE stock SET depid = 'dd1' WHERE depid = 'd1';")
    
    # 5. Adding a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock
    cur.execute("INSERT INTO product VALUES ('p100','cd', 5);")
    cur.execute("INSERT INTO stock VALUES ('p100', 'd2', 50);")

    # 6. Adding a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock
    cur.execute("INSERT INTO depot VALUES ('d100','Chicago', 100);")
    cur.execute("INSERT INTO stock VALUES ('p100', 'd100', 100);")
except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transaction could not be completed, rolling back...")
    con.rollback()

finally:
    if con:
        con.commit()
        cur.close()
        con.close()
        print("PostgreSQL connection is now closed")

print("End")

