
import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
reload(sys)
sys.setdefaultencoding('utf-8')

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#if sc is None:
#conf = SparkConf() \
#      .setAppName("GameLensALS") \
#      .set("spark.executor.memory", "2g")
#sc = SparkContext(conf=conf)
ratings = None
def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (gameID, ratings_iterable)
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

def trainALS(test, validation, training):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    rank = [4, 6, 8, 10, 12]
    errors = [0, 0, 0, 0, 0]
    min_error = float('inf')
    bestModel = None
    bestRank = -1
    #for i in range(0, len(rank)):
    temp_model = ALS.train(training, 10, 5, 0.01)
    #print ("%sth training is done!!!!" % i)
    predictions = temp_model.predictAll(validation.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
                           .join(validation.map(lambda x: ((x[0], x[1]), x[2]))) \
                           .values()
    errorValidation = sqrt(predictionsAndRatings.map(lambda x: (x[1] - x[0]) ** 2).mean())
    #print ("For rank %s Validation error is: %s" % (rank[i], errorValidation))

    #if errorValidation < min_error:
    #    min_error = errorValidation
    #    bestModel = temp_model
    #    bestRank = rank[i]

    print ('The best model was trained with rank %s' % bestRank)
    #print (predictionsAndRatings.take(1))
    #print (predictionsAndRatings.collect())


    return temp_model

def parseRating(line):
    """
    Parses a rating record in GameLens format userId::GameId::rating::timestamp .
    """
    fields = line.strip().split("::")
    return long(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))

#@staticmethod
def parseGame(line):
    """
    Parses a game record in GameLens format GameId::gameTitle .
    """
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]

#@staticmethod
def loadRatings(ratingsFile, spark_context):
    """
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print "File %s does not exist." % ratingsFile
        sys.exit(1)
    f = spark_context.textFile(ratingsFile)
    #print ("Miao Miao")
    #print (f.take(1))
    rating = f.map(lambda x: parseRating(x)).filter(lambda y: y[1][2] > 0)
    #print (ratings.take(2))
    #ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    #print ("Filtered rating result is:")
    #print (ratings)
    #f.close()
    if not rating:
        print "No ratings provided."
        sys.exit(1)
    else:
        return rating



def computeRmse(test, numTest):
    testPredictions = model.predictAll(test.map(lambda x: (x[0], x[1])))
    testPredictionsAndRatings = testPredictions.map(lambda x: ((x[0], x[1]), x[2]))\
                                .join(test.map(lambda y: ((y[0], y[1]), y[2]))).values()
    errorTest = sqrt(testPredictionsAndRatings.map(lambda x: (x[1] - x[0]) ** 2).mean())
    print (testPredictionsAndRatings.collect())
    return errorTest

def count_and_average_ratings():
    """Updates the movies ratings counts from
    the current data self.ratings_RDD
    """
    logger.info("Counting movie ratings...")
    #print (ratings.take(2))
    games_ID_with_ratings_RDD = ratings.map(lambda x: (x[1][1], x[1][2])).groupByKey()
    games_ID_with_avg_ratings_RDD = games_ID_with_ratings_RDD.map(get_counts_and_averages)
    #print (games_ID_with_avg_ratings_RDD.take(2))
    global games_rating_counts_RDD
    games_rating_counts_RDD = games_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))
    #games_rating_counts_RDD.take(2)


def add_ratings(new_ratings):
    # Convert ratings to an RDD
    new_ratings_RDD = sc.parallelize(new_ratings)
    #new_ratings_RDD = new_ratings_RDD
    # Add new ratings to the existing ones
    #new_ratings = ratings.union(new_ratings_RDD)
    
    trainingOld, validationOld, testOld = ratings.randomSplit([6.0, 2.0, 2.0], 24)
    print(trainingOld.take(2))
    test = testOld.map(lambda x: (x[1][0], x[1][1], x[1][2]))
    training = trainingOld.map(lambda x: (x[1][0], x[1][1], x[1][2])).union(new_ratings_RDD)
    #.map(lambda y: Rating(y[0], y[1], y[2]))
    validation = validationOld.map(lambda x: (x[1][0], x[1][1], x[1][2])
    )
    print ("MIAOMIAO")
    print (training.take(2))
    print (validation.take(2))
    print (test.take(2))
    model = trainALS(test, validation, training)
    #candidates = sc.parallelize([m for m in games if m not in gameToExcludeIds]).map(lambda x: (0, x))

    gameToExcludeIds = new_ratings_RDD.map(lambda x: x[1]).distinct()
    candidates = games.map(lambda x: x[0]).subtract(gameToExcludeIds)

    toBePredicted = candidates.map(lambda x: (0, x))
    predictions = model.predictAll(toBePredicted)

    predictionSorted = sorted(predictions.collect(), key=lambda x: x[2], reverse=True)[:10]


    #Save model
    #model.write().overwrite().save(sc, "/home/ubuntu/gamesRecommenderSpark/gamesRecommenderSpark/model")
    #sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
    print ("Predictions")
    print (predictionSorted)
    gameDict = dict(games.collect())
    for prediction in predictionSorted:
        print (gameDict[prediction[1]])


    # clean up
    #sc.stop()

def predict_ratings(model, user_and_game_RDD):
    """Gets predictions for a given (userID, movieID) formatted RDD
    Returns: an RDD with format (movieTitle, movieRating, numRatings)
    """
    predicted_RDD = model.predictAll(user_and_game_RDD)
    predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
    predicted_rating_title_and_count_RDD = \
        predicted_rating_RDD.join(games_titles_RDD).join(games_rating_counts_RDD)
    predicted_rating_title_and_count_RDD = \
        predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

    return predicted_rating_title_and_count_RDD

def get_top_ratings(model, user_id, games_count):
    """Recommends up to movies_count top unrated movies to user_id
    """
    # Get pairs of (userID, movieID) for user_id unrated movies
    user_unrated_game_RDD = games.filter(lambda rating: not rating[1]==user_id).map(lambda x: (user_id, x[0]))
    # Get predicted ratings
    rating = predict_ratings(model, user_unrated_game_RDD).filter(lambda r: r[2]>=25).takeOrdered(games_count, key=lambda x: -x[1])

    return rating

def get_ratings_for_game_ids(user_id, game_ids):
    """Given a user_id and a list of movie_ids, predict ratings for them
    """
    requested_games_RDD = sc.parallelize(game_ids).map(lambda x: (user_id, x))
    # Get predicted ratings
    rating = predict_ratings(requested_games_RDD).collect()

    return rating

class RecommendationEngine:
    def predict_ratings(self, sc, model, user_and_game_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        gameLensHomeDir = "/home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/medium"
        global games
        games = sc.textFile(join(gameLensHomeDir, "games.dat")).map(parseGame).cache()
        print (games.take(2))

        global games_titles_RDD
        games_titles_RDD = games.map(lambda x: (int(x[0]),x[1])).cache()

        predicted_RDD = model.predictAll(user_and_game_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(games_titles_RDD).join(games_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD
    def get_ratings_for_game_ids(self, sc, model, user_id, game_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them
        """
        requested_games_RDD = sc.parallelize(game_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        rating = self.predict_ratings(sc, model, requested_games_RDD).collect()

        return rating
    def gen_model(self, sc):
        #myRatings = loadRatings("/home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/personalRatings.txt", sc)
        myRatings = loadRatings("/home/ubuntu/efs/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/personalRatings.txt", sc)
        #gameLensHomeDir = "/home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/medium"
        gameLensHomeDir = "/home/ubuntu/efs/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/medium"
        # ratings is an RDD of (last digit of timestamp, (userId, gameId, rating))
        global ratings
        ratings = sc.textFile(join(gameLensHomeDir, "ratings.dat")).map(parseRating).cache()
        print (ratings.take(2))
        # games is an RDD of (gameId, gameTitle)
        #games = dict(sc.textFile(join(gameLensHomeDir, "games.dat")).map(parseGame).collect())
        global games
        games = sc.textFile(join(gameLensHomeDir, "games.dat")).map(parseGame).cache()
        print (games.take(2))

        global games_titles_RDD
        games_titles_RDD = games.map(lambda x: (int(x[0]),x[1])).cache()
        # your code here
        #print ("The first two lines of ratings")
        #print (ratings.take(2))
        numRatings = ratings.count()
        #print ("CAOCAOCAOCAOCAO")
        #print (ratings.values().map(lambda x: x[0]).take(2))
        #print (ratings.map(lambda x: x[1][0]).take(2))
        numUsers = ratings.values().map(lambda x: x[0]).distinct().count()
        numGames = ratings.values().map(lambda x: x[1]).distinct().count()


        print "Got %d ratings from %d users on %d games." % (numRatings, numUsers, numGames)

        #trainingOld, validationOld, testOld = ratings.randomSplit([6.0, 2.0, 2.0], 24)
        trainingOld, validationOld, testOld = ratings.randomSplit([6.0, 2.0, 2.0], 24)

        #MIAO
        training = trainingOld.union(myRatings).map(lambda x: Rating(x[1][0], x[1][1], x[1][2]))

        test = testOld.map(lambda x: (x[1][0], x[1][1], x[1][2]))
        #training = trainingOld.map(lambda x: Rating(x[1][0], x[1][1], x[1][2]))
        validation = validationOld.map(lambda x: (x[1][0], x[1][1], x[1][2]))
        numTraining = training.count()
        numValidation = validation.count()
        numTest = test.count()


        print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)
        count_and_average_ratings()
        #rank = 10
        global model
        model = trainALS(test, validation, training)
        #model.save(sc, "/home/ubuntu/gamesRecommenderSpark/gamesRecommenderSpark/model")
        model.save(sc, "/home/ubuntu/efs/gamesRecommenderSpark/gamesRecommenderSpark/model")
        testRmse = computeRmse(test, numTest)
        print "Our model has a RMSE of %s" % (testRmse)
        gameToExcludeIds = myRatings.map(lambda x: x[1][1]).distinct()

        candidates = games.map(lambda x: x[0]).subtract(gameToExcludeIds)

        toBePredicted = candidates.map(lambda x: (0, x))
        predictions = model.predictAll(toBePredicted)

        predictionSorted = sorted(predictions.collect(), key=lambda x: x[2], reverse=True)[:10]
        print ("Predictions")
        print (predictionSorted)
        gameDict = dict(games.collect())
        game_predictions = []
        with open("/home/ubuntu/flaskapp/static/recommend.html", 'w') as fh:
            fh.write("<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/style.css\">\n")
            fh.write("<h1> Games recommended</h1>\n")
            for prediction in predictionSorted:
                print (gameDict[prediction[1]])
                game_predictions.append(gameDict[prediction[1]])
                fh.write("<h3 class=\"prediction\">" + gameDict[prediction[1]] + "</h3>\n")
            #return game_predictions

    def __init__(self, sc):
        #if (len(sys.argv) != 3):
        #    print "Usage: /path/to/spark/bin/spark-submit --driver-memory 2g " + \
        #      "GameLensALS.py GameLensDataDir personalRatingsFile"
        #    sys.exit(1)
    
        # set up environment
    
    
        # load personal ratings
        #print (SparkContext)
        self.sc = sc
    
if __name__ == "__main__":
    global recommendation_engine
    recommendation_engine = RecommendationEngine(sc)
    recommendation_engine.gen_model(sc)
#init(sc)
