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

import sys
from operator import add
from pyspark import SparkContext
import unicodedata

# define user information
def definition():
    global user_id
    user_id = -2
    global top_ratings
    top_ratings = []
    global top_index
    top_index = -2


@main.route('/')
def login():
    server_ip = str(requests.get('http://bot.whatismyipaddress.com/').text)
    return render_template('login.html', server_ip=server_ip)

# user's first music
@main.route('/music/<int:uid>')
def firstpage(uid):
    global top_index
    global user_id
    global top_ratings
    top_index = -1
    user_id = uid
    top_ratings = []
    artist = 'magica'  # start from magica
    trackurl = get_track_url(artist)
    server_ip = str(requests.get('http://bot.whatismyipaddress.com/').text)
    return render_template('music.html', url=trackurl, this_artist=artist, server_ip=server_ip)

# save current rating and get a new artist's music
@main.route('/<artist>/<int:rating>')
def nextpage(artist, rating):
    global top_index
    global user_id
    global top_ratings
    save_score(artist, rating)  # save the rating for the previous artist
    artist = get_next_artist()  # get the next artist name
    trackurl = get_track_url(artist)
    server_ip = str(requests.get('http://bot.whatismyipaddress.com/').text)
    return render_template('music.html', url=trackurl, this_artist=artist, server_ip=server_ip)

# save new ratings into current spark context
@main.route("/ratings/<int:user_id>", methods=["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, music_id,
    # rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)

    return json.dumps(ratings)

# get a track from artist name
def get_track_url(artist):
    api_artist = 'https://api.spotify.com/v1/search?q=' + \
        artist + '&type=artist&market=US&limit=1'
    artistjson = requests.get(api_artist).json()
    artistid = artistjson['artists']['items'][0]['id']
    artistid = str(artistid)
    api_track = 'https://api.spotify.com/v1/artists/' + \
        artistid + '/top-tracks?country=US&limit=1'
    trackjson = requests.get(api_track).json()
    if trackjson['tracks'] != []:
        trackid = trackjson['tracks'][0]['id']
    else:
        api_album = 'https://api.spotify.com/v1/artists/' + artistid + '/albums?limit=1'
        albumjson = requests.get(api_album).json()
        albumid = albumjson['items'][0]['id']
        api_track = 'https://api.spotify.com/v1/albums/' + albumid + '/tracks?limit=1'
        trackjson = requests.get(api_track).json()
        trackid = trackjson['items'][0]['id']
    trackurl = 'https://embed.spotify.com/?uri=spotify%3Atrack%3A' + trackid
    return trackurl

# save the rating into sql database
def save_score(artist, rating):
    db = MySQLdb.connect("localhost", "root", "root", "mr")
    cursor = db.cursor()

    try:
        sql = 'select * from artists where name="%s"' % (artist)
        cursor.execute(sql)
        result = (cursor.fetchall())
        aid = result[0][0]
    except:
        return 'No such artist!'

    # user0 is the database for user 0
    try:
        sql = 'insert into user0 (aid,rating) values ("%d","%f")' % (
            aid, rating)
        cursor.execute(sql)
    except:
        sql = 'update user0 SET rating="%f" where aid="%d"' % (rating, aid)
        cursor.execute(sql)
    db.commit()
    db.close()

    with open('user_1_new_ratings.file', 'a') as file:
        line = str(aid) + ',' + str(rating) + '\n'
        file.write(line)

    return 'Saved!'

# return a recommended artist
def get_next_artist():
    logger.debug("User %s TOP rating predicting...", user_id)
    global top_index
    global user_id
    global top_ratings

    if (top_index + 1) % 20 == 0:
        # add the new ratings into spark context
        bashCommand = 'curl --data-binary @user_1_new_ratings.file http://0.0.0.0:5432/ratings/1'
        process = subprocess.Popen(bashCommand.split())
        # get top recommended artists for user
        top_ratings = recommendation_engine.get_top_ratings(user_id, 21)
    top_index = (top_index + 1) % 20
    artist_rcmd = top_ratings[top_index][0]
    return artist_rcmd

#get keywords of a user's favorite songs
def get_music_bubbles(ratings_filename):
    sc2 = SparkContext(appName="MusicBubble")
    lines = sc2.textFile(ratings_filename, 1)
    counts = line.flatMap(lambda x: x.split('')).map(
        lambda x: (x, 1)).reduceByKey(add)
    output = counts.collect()
    bubbles = []
    for (word, count) in output:
        word_a = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore')
        item = [word_a, count]
        bubbles.append(item, key=item[1])
    sc.stop()
    return bubbles

# init/main function
def create_app(spark_context, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)

    definition()
    return app
