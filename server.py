import time
import sys
import cherrypy
import os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf


def init_spark_context():
    # load spark context
    configuration = SparkConf().setAppName("music_recommendation-server")

    # IMPORTANT: pass aditional Python modules to each worker
    sparkengine = SparkContext(conf=configuration, pyFiles=[
                               'engine.py', 'app.py'])

    return sparkengine


def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    # Init spark context and load libraries
    sparkengine = init_spark_context()
    #dataset_path = os.path.join('datasets', 'ml-latest')
    dataset_path = 'file:///home/hadoop/Downloads/server/datasets/'
    app = create_app(sparkengine, dataset_path)

    # start web server
    run_server(app)
