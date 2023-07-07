from main_webshop import *


postgres_lijst = data

def filter_brand_category(profile_id, connection_list, brand, category):
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE _id='23978'")
    main = cur.fetchone()

    result = []

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM products WHERE brand = (%s) AND category = (%s)", (brand, category))
        products = cur.fetchmany(4)
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
    return result

# Voorbeeldgebruik:
brand = "Merknaam"
category = "Categorie"
filter_brand_category('1', postgres_lijst, "8x4", "Gezond & verzorging")