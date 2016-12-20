import json
import requests
import subprocess
import MySQLdb
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from engine import RecommendationEngine

from flask import Flask, request, render_template
from flask import Blueprint
main = Blueprint('main', __name__)


# return top 5 recommendations with ratings to the master server
@main.route("/")
def add_ratings():
    user_id = request.args.get('uid')
    count = request.args.get('count')
    top_ratings = recommendation_engine.get_top_ratings(user_id, count)
    return top_ratings


@main.route("/add_rating/<int:user_id>/", methods=["POST"])
# save new ratings into current spark context
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, music_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)

    return ratings


# init/main function
def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)
    app = Flask(__name__)
    app.register_blueprint(main)
    definition()
    return app
