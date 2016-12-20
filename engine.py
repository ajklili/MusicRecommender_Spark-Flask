import os
import pyspark
from pyspark.mllib.recommendation import ALS

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Bigdata music recommendation engine
class RecommendationEngine:
    # Initialize the recommendation engine given a spark context
    def __init__(self, sc, dataset_path):        
        logger.info("===Starting up the Recommendation Engine===")
        self.sc = sc

        # Load ratings data for later use
        logger.info("===Loading ratings data from files===")
        ratings_file_name = 'ratings.csv'
        ratings_file_path = os.path.join(dataset_path, ratings_file_name)
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        self.ratings_RDD = ratings_raw_RDD.filter.map(lambda line: line.split(",")).map(
            lambda items: (int(items[0]), int(items[1]), float(items[2]))).cache()
        # Load artists data for later use
        logger.info("===Loading artists index data from files===")
        artists_file_name = 'artists.csv'
        artists_file_path = os.path.join(dataset_path, artists_file_name)
        artists_raw_RDD = self.sc.textFile(artists_file_path)
        self.artists_RDD = artists_raw_RDD.filter.map(lambda line: line.split(
            ",")).map(lambda items: (int(items[0]), items[1])).cache()
        self.artists_titles_RDD = self.artists_RDD.map(
            lambda x: (int(x[0]), x[1])).cache()

        # Pre-calculate artists ratings counts
        logger.info("===Counting music ratings===")
        music_ID_with_ratings_RDD = self.ratings_RDD.map(
            lambda x: (x[1], x[2])).groupByKey()
        music_ID_with_avg_ratings_RDD = music_ID_with_ratings_RDD.map(
            get_counts_and_averages)
        self.artists_rating_counts_RDD = music_ID_with_avg_ratings_RDD.map(
            lambda x: (x[0], x[1][0]))

        # Train the model
        logger.info("===Training the ALS model===")
        self.rank = 10
        self.seed = 5L
        self.iterations = 20
        self.regularization_parameter = 0.2
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("---ALS model built---")

    # Gets predictions for a given (userID, musicID) formatted RDD
    def predict_ratings(self, user_and_music_RDD):
        predicted_RDD = self.model.predictAll(user_and_music_RDD)
        predicted_rating_RDD = predicted_RDD.map(
            lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.artists_titles_RDD).join(
                self.artists_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(
                lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        return predicted_rating_title_and_count_RDD

    # Add additional music ratings in the format (user_id, music_id, rating)
    def add_ratings(self, ratings):
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute music ratings count
        logger.info("===Counting music ratings===")
        music_ID_with_ratings_RDD = self.ratings_RDD.map(
            lambda x: (x[1], x[2])).groupByKey()
        music_ID_with_avg_ratings_RDD = music_ID_with_ratings_RDD.map(
            get_counts_and_averages)
        self.artists_rating_counts_RDD = music_ID_with_avg_ratings_RDD.map(
            lambda x: (x[0], x[1][0]))

        # Train again the ALS model with the new ratings
        logger.info("===Training the ALS model===")
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("---ALS model refreshed---")
        return ratings

    # Given a user_id and a list of artistIds, predict ratings for them
    def get_ratings_for_artistIds(self, user_id, artistIds):
        target_artists_RDD = self.sc.parallelize(
            artistIds).map(lambda x: (user_id, x))

        # Get predicted ratings
        ratings = self.predict_ratings(target_artists_RDD).collect()
        return ratings

    # Get pairs of (userID, artistID)
    def get_top_ratings(self, user_id, artists_count):
        user_unrated_artists_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
            .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.predict_ratings(user_unrated_artists_RDD).filter(
            lambda r: r[2] >= 25).takeOrdered(artists_count, key=lambda x: -x[1])
        return ratings


def get_counts_and_averages(ID_and_ratings):
    number_of_ratings = len(ID_and_ratings[1])
    i_r = number_of_ratings, float(
        sum(x for x in ID_and_ratings[1])) / number_of_ratings
    return ID_and_ratings[0], i_r
