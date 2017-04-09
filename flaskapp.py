from flask import url_for, redirect, Flask, request, Blueprint
from engine import RecommendationEngine
#import engine
app = Flask(__name__)
from pyspark import SparkConf, SparkContext
#main = Blueprint('main', __name__)
from flask import render_template 
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import subprocess
import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
#@app.route("/<int:user_id>/ratings/top/", methods=["GET"])
#def top_ratings(user_id):
#    logger.debug("User %s TOP ratings requested", user_id)
#    top_ratings = recommendation_engine.get_top_ratings(user_id)
#    return json.dumps(top_ratings)
conf = SparkConf().setAppName("game_recommendation-server")
# IMPORTANT: pass aditional Python modules to each worker
sc = SparkContext(conf=conf)
sc.addPyFile('/home/ubuntu/flaskapp/engine.py')
sc.addPyFile('/home/ubuntu/flaskapp/flaskapp.py')

@app.route('/')
def hello_world():
    return render_template("helloworld.html")

@app.route("/print")
def print_recommended_game():
    return redirect(url_for('static', filename='recommend.html'))
    #return render_template("recommendations.html")
sameModel = None
recommendation_engine = None
@app.route('/init')
def init():
    #os.system("/usr/lib/spark/bin/spark-submit /home/ubuntu/flaskapp/engine.py -driver-memory 4g")
    global recommendation_engine
    recommendation_engine = RecommendationEngine(sc)
    #global sameModel
    if os.path.isdir("/home/ubuntu/gamesRecommenderSpark/gamesRecommenderSpark/model"):
        global sameModel
        sameModel = MatrixFactorizationModel.load(sc, "/home/ubuntu/efs/gamesRecommenderSpark/gamesRecommenderSpark/model")
    else:
        logger.debug("HAHAHAHA!!!! MIAOMIAOMIAOMIAO!!!!")
        #child = subprocess.call(["/usr/lib/spark/bin/spark-submit", "--driver-memory", "4g", "/home/ubuntu/flaskapp/engine.py", "/home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/medium", "/home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/personalRatings.txt"])
        #child.wait()
        #child = os.system("/usr/lib/spark/bin/spark-submit --driver-memory 4g /home/ubuntu/flaskapp/engine.py /home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/medium /home/ubuntu/Game_Recommendation/gamesRecommenderSpark/pythonGames/data/gamelens/personalRatings.txt")
        #print (child)
        global sameModel
        sameModel = MatrixFactorizationModel.load(sc, "/home/ubuntu/efs/gamesRecommenderSpark/gamesRecommenderSpark/model")
    logger.debug("parent process")
    #global recommendation_engine 
 
    #recommendation_engine = RecommendationEngine(sc)    
    #return app
    return 'Hello from Game Recommendation System!'

@app.route('/showinput/<input_str>')
def show_input(input_str):
    return input_str


@app.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    #top_ratings = get_top_ratings(sameModel, user_id, count)
    top_ratings = ratings = sameModel.recommendProducts(user_id, count)
    return json.dumps(top_ratings)
 
@app.route("/<int:user_id>/ratings/<int:game_id>", methods=["GET"])
def game_ratings(user_id, game_id):
    logger.debug("User %s rating requested for game %s", user_id, game_id)
    #ratings = recommendation_engine.get_ratings_for_game_ids(sc, sameModel, user_id, [game_id])
    ratings = sameModel.recommendProducts(user_id, 5)
    return render_template("recommendations.html", predictions = ratings)
 
 
@app.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, game_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    add_ratings(ratings)
 
    return json.dumps(ratings)
 
#@main.route('/') 
#def create_app(spark_context):
#    global recommendation_engine 
# 
#    recommendation_engine = RecommendationEngine(spark_context)    
#    
#    app = Flask(__name__)
#    app.register_blueprint(main)
#    return app

if __name__ == "__main__":
    #global recommendation_engine
    #recommendation_engine = RecommendationEngine(sc)
    #os.system("/usr/lib/spark/bin/spark-submit /home/ubuntu/flaskapp/engine.py -driver-memory 4g")
    #if not os.path.isdir("/home/ubuntu/gamesRecommenderSpark/gamesRecommenderSpark/model"):
    #    print ("HAHAHAHA!!!! MIAOMIAOMIAOMIAO!!!!")
    #    subprocess.call(["/usr/lib/spark/bin/spark-submit", "/home/ubuntu/flaskapp/engine.py", "-driver-memory", "4g"])
    app.run(threaded=True)
