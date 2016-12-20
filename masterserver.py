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


def definition():
    # define user information
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


@main.route('/music/<int:uid>')
# user's first music
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


@main.route('/<artist>/<int:rating>')
# save current rating and get a new artist's music
def nextpage(artist, rating):
    global top_index
    global user_id
    global top_ratings
    save_score(artist, rating)  # save the rating for the previous artist
    artist = get_next_artist()  # get the next artist name
    trackurl = get_track_url(artist)
    server_ip = str(requests.get('http://bot.whatismyipaddress.com/').text)
    return render_template('music.html', url=trackurl, this_artist=artist, server_ip=server_ip)


def get_track_url(artist):
    # get a track from artist name
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


def save_score(artist, rating):
    # save the rating into sql database
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

    user_new_ratings_filename = 'user_' + user_id + '_new_ratings.file'
    with open(user_new_ratings_filename, 'a') as file:
        line = str(aid) + ',' + str(rating) + '\n'
        file.write(line)

    return 'Saved!'


def get_next_artist():
    # return a recommended artist
    logger.debug("User %s TOP rating predicting...", user_id)
    global top_index
    global user_id
    global top_ratings

    if (top_index + 1) % 20 == 0:
        # get top recommended artists for user
        top_ratings = get_reccommended_artists(user_id, 10)
        top_ratings = top_ratings[0:21]
    top_index = (top_index + 1) % 20
    artist_rcmd = top_ratings[top_index][0]
    return artist_rcmd


def get_reccommended_artists(user_id, count):
    top_ratings = []
    slave_server_list = []
    slave_server_list.append('54.91.116.125')
    slave_server_list.append('52.201.212.210')
    slave_server_list.append('52.110.38.64')
    slave_server_list.append('54.78.44.202')
    # add the new ratings into spark context
    user_new_ratings_filename = 'user_' + user_id + '_new_ratings.file'
    save_new_ratings(user_new_ratings_filename, slave_server_list)
    params = {'uid': user_id, 'count' = count}
    for ip in slave_server_list:
        url = 'http://' + ip + '/'
        part_ratings = requests.get(url, params=params).json()
        for rating in part_ratings:
            top_ratings.append(rating)
        top_ratings = sorted(top_ratings, key=rating[2])
    return top_ratings


def save_new_ratings(ratings_filename, slave_server_list):
    user_id=ratings_filename[5]
    # add the new ratings into spark context
    for ip in slave_server_list:
        bashCommand = 'curl --data-binary @' + \
            ratings_filename + ' http://' + ip + ':5432/add_rating/'+user_id+'/'
        process = subprocess.Popen(bashCommand.split())


def create_app(spark_context, dataset_path):
    # init/main function
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    definition()
    return app
