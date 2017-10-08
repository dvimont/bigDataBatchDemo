## WikiTrends: Using Big Data tools to bring the Wikipedia data lake to life!

### View [the video presentation](https://youtu.be/yBw36dquUfw)

### Project overview:
The WIKITRENDS project is all about giving data scientists and web developers easy access to aggregated trending data derived from the Wikimedia Foundation's* [public data lake](https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews). The biggest challenge was the quantity: it called for the aggregation and transformation of 3TB of Wikipedia hourly pageview data, ultimately to allow simple queries for the top 500 webpages on a given day, month, or year. The project uses S3 and Hadoop for data storage, Spark for aggregation & transformation, and ElasticSearch for a low-latency datastore and simple querying.

&nbsp;&nbsp;&nbsp;&nbsp;*the Wikimedia Foundation = the Wikipedia people

This package consists of components used to complete the following sequence:
* Download the complete set of pageviews (hourly summary files, 1TB compressed) from Wikimedia's servers and save the files to S3 (cold storage).
* Transfer the files to HDFS (processing storage).
* Execute Spark processing to generate daily, weekly, and yearly aggregations and output data on the 500 most popular sites for a given day, month, or year.
* Load the summary data into ElasticSearch for indexing and to provide a basic public API, and also to offer graphical presentations on the data using Kibana.

### Command line execution of the processing sequence is as follows:
1. **loadRemoteData** (downloads raw-data [gzipped] files from Wikimedia servers and loads into S3)  
   `./src/main/bin/loadRemoteData [processing-limit]`  
     e.g., `./src/main/bin/loadRemoteData 0`  
   *[processing-limit] parameter sets limit of raw-data files per month to download; when set to "0", everything is downloaded.*
2. **moveS3ToHdfsDistcp** (copies raw-data files from S3 [cold storage] to HDFS [processing storage])  
   `nohup ../src/main/bin/moveS3ToHdfsDistcp [year] [start-month] [end-month] &`  
     e.g., `nohup ./src/main/bin/moveS3ToHdfsDistcp 2016 01 12 &`  
   *Note that nohup invocation is recommended (could run for several minutes).*
3. **runSparkJobs** (inputs raw, hourly pageview data; outputs 500 most popular Wikipedia webpages on daily, monthly, and yearly basis, in JSON format ready for loading into ElasticSearch)  
   `nohup ./src/main/bin/runSparkJobs [storage-level] [executor-mem] [spark-master-url:port] [hadoop-master-url:port] [year] &`  
     e.g., `nohup ./src/main/bin/runSparkJobs MEMDISK 40g spark://url-goes-here:7077 hdfs://url-goes-here:9000/ 2016 &`  
   *Note that nohup invocation is recommended (could run for up to 2 hours).*
4. **zipForES** (extracts JSON outputted by Spark job and puts into tar/gzip format)  
   `./src/main/bin/zipForES [hdfs_dir_of_spark_output] [tarzip_file_prefix]`  
     e.g., `./src/main/bin/zipForES /output/ xferToES`  
   *The above example will result in a file named `xferToES.tar.gz` being outputted.*
5. **File transfer and unzip**  
   *Use scp to transfer file to ElasticSearch master, and invoke `gunzip` and `tar -xvf` to unzip and untar the file.*
6. **loadES** (run on ElasticSearch master node, invokes ElasticSearch bulk-loader utility to load all JSON docs into ES.)  
   `./src/main/bin/loadES [index] [type] [inputDirectory]`  
     e.g., `./src/main/bin/loadES popular-pages interval_type ~/import/xferToES/`

