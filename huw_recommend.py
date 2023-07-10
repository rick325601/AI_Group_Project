from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from recommendations_webshop import *

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER","MONGODBPASSWORD","MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    if envvals[1] != '':
        dbstring = dbstring.format(*envvals)
    else:
        dbstring = 'mongodb://{0}/{1}'.format(envvals[2], 'test')
    client = MongoClient(dbstring)
else:
    client = MongoClient()
database = client.huwebshop

class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    def get(self, profileid, count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        randcursor = database.products.aggregate([{'$sample': {'size': count}}])
        prodids = list(map(lambda x: x['_id'], list(randcursor)))

        # Call recommendation script to get recommended products
        current_product_id = prodids[0]  # Assuming the first product ID from the random sample
        recommendations = collaborative_filtering(postgres_lijst, current_product_id)

        # Extract the product IDs from the recommendations
        recommended_prodids = [product[0] for product in recommendations]

        return recommended_prodids, 200

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<int:count>")

if __name__ == '__main__':
    app.run(debug=True)
