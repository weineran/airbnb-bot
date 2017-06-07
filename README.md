# Airbnb Bot

## Boxes and Arrows
https://docs.google.com/presentation/d/1BAVQ4aTnTCOD8iiBl3Q-AL68n_0g0FVQCe0OpD4Cc0g/edit?usp=sharing

## Getting Started
* Clone the repo.
* Run the multicontainer docker app:  
`$ docker-compose up --build -d`  
This consists of a MongoDB container and a Zeppelin container.  This will take a while the first time as it downloads the MongoDB and Zeppelin docker images.

* List the currently running docker containers:  
`$ docker ps`  
You should see `airbnbbot_zeppelin` and `airbnbbot_mongodb` listed.

* Confirm that Zeppelin is running by opening the web interface: http://localhost:8080/
  * Click on the **Notebook** dropdown
  * Click on **Zeppelin Tutorial**
  * Click on **Basic Features (Spark)**
  * You will see a list of interpreters. They are fine--click the "Save" button.
  * Each section is called a "Paragraph" and contains a play button.  In the first paragraph, click the play button.
  * If that seems to work, click the play button for the next paragraph, and so on.  Make sure you wait for each paragraph to finish before running the next paragraph.
* Confirm that MongoDB is running:  
  * `$ docker exec -it airbnbbot_mongodb_1 mongo` (This opens a MongoDB shell on your MongoDB docker container)  
  * `> show dbs` (Show databases if you're curious)  
  * `> exit` (Exits the MongoDB shell and returns you to your terminal)  

## Copy data into local MongoDB docker container
The `copy_mongo.py` script will copy data from a remote MongoDB to your locally running MongoDB docker container.

**NOTE**: You will need to place a `ca.pem` file in the repo directory to connect to the remote MongoDB.  The `ca.pem` file will be provided to you by the workshop presenters.

Once you have the `ca.pem` file, you can start copying data, which will take a while.  For example, try:  
`$ python copy_mongo.py python copy_mongo.py --start-date=20170601` (Copies data from 6/1/2017 thru the current date)

For more usage info:  
`$ python copy_mongo.py --help`

## Try out Spark Connector in Zeppelin
You can do this while you are waiting for the data copy to complete.

**SIDE NOTE**: Here are some pointers to additional info on the Spark Connector:  
SparkPackages: https://spark-packages.org/package/mongodb/mongo-spark  
Docs: https://docs.mongodb.com/spark-connector/current/  
GitHub: https://github.com/mongodb/mongo-spark

Instructions:
* Add the MongoDB Spark Connector to Zeppelin's Spark interpreter:
  * Go to the Zeppelin interprer menu: http://localhost:8080/#/interpreter
  * In the search box, type "spark" to find the spark interpreter.
  * Click the "edit" button.
  * Scroll down to the **Dependencies** section.
  * Under **artifact**, in the text box paste `org.mongodb.spark:mongo-spark-connector_2.11:2.0.0`
  * Click the "Save" button

* Import a Zeppelin notebook that uses the Spark Connector:
  * Go to the welcome page: http://localhost:8080/#/
  * Click "Import note".
  * Click the "Choose a JSON here" button.
  * Navigate to your repo directory and select the `spark-connector-tutorial.json` file.
  * You should be back on the welcome page now.  In the **Notebook** dropdown, select "Spark Connector Tutorial".
  * Read through the first paragraph--it is heavily commented.
  * Try running the paragraphs.
