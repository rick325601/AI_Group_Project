from main_webshop import *
from data import data
import random

postgres_lijst = data

def filter_brand_category(profile_id, connection_list, brand, category):
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE _id='23978'")

    result = []

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM products WHERE brand = (%s) AND category = (%s)", (brand, category))
        products = cur.fetchall()
        print("The number of products: ", cur.rowcount)
        for product in products:
            print(product)
            result.append(product[0])
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    # Willekeurige aanbevelingen selecteren
    random.shuffle(result)
    recommended_products = result[:5]

    return recommended_products

# Voorbeeldgebruik:
brand = "8x4"
category = "Gezond & verzorging"
recommendations = filter_brand_category('1', postgres_lijst, brand, category)
print(recommendations)

