from main_webshop import *
from data import data
import random
import psycopg2

postgres_lijst = data

def collaborative_filtering(connection_list, current_product_id):
    """
    Performs collaborative filtering to recommend similar products based on the given product ID.

    Args:
        connection_list (list): List containing connection parameters for PostgreSQL.
        current_product_id (str): ID of the current product.

    Returns:
        list: List of recommended products (tuples of product ID, product name).

    """
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    # Haal informatie op over het huidige product
    cur.execute("SELECT * FROM products WHERE _id = (%s)", (current_product_id,))
    current_product = cur.fetchone()

    # Filter de producten op basis van vergelijkbare kenmerken
    cur.execute("SELECT * FROM products WHERE price BETWEEN (%s) AND (%s) AND category = (%s) AND sub_category = (%s) AND brand = (%s)",
                (current_product[1] - 50, current_product[1] + 50, current_product[4], current_product[5], current_product[10]))
    similar_products = cur.fetchall()

    # Willekeurige aanbevelingen selecteren
    random.shuffle(similar_products)
    recommended_products = similar_products[:5]

    return recommended_products

# Voorbeeldgebruik:
current_product_id = '23978'
recommendations = collaborative_filtering(data, current_product_id)
print("Collaborative Filtering:")
for product in recommendations:
    print("Product ID:", product[0])
    print("Product Name:", product[2])
    print()


def filter_brand_category_id(connection_list, product_id):
    """
    Filters products based on the brand and category of the given product ID.

    Args:
        connection_list (list): List containing connection parameters for PostgreSQL.
        product_id (str): ID of the product.

    Returns:
        list: List of recommended products (tuples of product ID, product name).

    """
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE _id = (%s)", (product_id,))

    result = []

    try:
        products = cur.fetchall()
        for product in products:
            brand = product[10]
            category = product[4]
            cur.execute("SELECT * FROM products WHERE brand = (%s) AND category = (%s)", (brand, category))
            similar_products = cur.fetchall()
            for similar_product in similar_products:
                result.append(similar_product)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    random.shuffle(result)
    recommended_products = result[:5]

    return recommended_products

# Voorbeeldgebruik:
product_id = '23869'
recommendations = filter_brand_category_id(data, product_id)
print("Content Filtering:")
for product in recommendations:
    print("Product ID:", product[0])
    print("Product Name:", product[2])
    print()
