## WikiTrends: Using Big Data tools to bring the Wikipedia data lake to life!

### The goal of this project:
Aggregate and index 3TB of Wikipedia page-view data (downloaded from the Wikimedia Foundation's public data lake, and enable low-latency daily, monthly, & yearly aggregated queries via a simple public API.


This package consists of components used to complete the following sequence:
* Download the complete set of pageviews (hourly summary files, 1TB compressed) from Wikimedia's servers and save the files to S3 (cold storage).
* Transfer the files to HDFS (processing storage).
* Execute Spark processing to generate daily, weekly, and yearly aggregations and output data on the 500 most popular sites for a given day, month, or year.
* Load the summary data into ElasticSearch for indexing and to provide a basic public API, and also to offer graphical presentations on the data using Kibana.

INSTRUCTIONS ON COMMAND LINE EXECUTION OF EACH STEP, COMING SOON!