/*
 * Copyright 2017 Daniel Vimont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonvox.bigdatademos;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 *
 * @author Daniel Vimont
 */
public class SparkDriver {
    
//    private static final String hdfsNamenode =
//            "hdfs://ec2-54-164-189-32.compute-1.amazonaws.com:50070/";

    public static void main( String[] args ) throws Exception {
        if (args.length < 3) {
          System.err.println("Usage: PageViewsDaily <hdfs-master url> <input path> <output path>");
          System.exit(-1);
        }
        String hdfsNamenode = args[0];
        String inputHdfsFile = args[1];
        String outputHdfsFile = args[2];
        
        SparkConf conf = new SparkConf().setAppName("PageViewsDaily");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = 
             (JavaNewHadoopRDD) sc.newAPIHadoopFile(hdfsNamenode +
                        inputHdfsFile,  // e.g. "test/raw_files", 
                TextInputFormat.class,  // format of the inputted file data
                LongWritable.class,     // key class
                Text.class,             // value class
                new Configuration()     // hadoop config
        );
        JavaRDD<Tuple2<String, Integer>> pageViewsDaily =
                hadoopRDD.mapPartitionsWithInputSplit(new DailyMapper(), true);

        // Output result to HDFS 
        pageViewsDaily.saveAsTextFile(hdfsNamenode + outputHdfsFile); // "test/pageviews.daily");
    
    }
    
    static class DailyMapper implements Function2<InputSplit,
            Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, Integer>>> {
        @Override
        public Iterator<Tuple2<String, Integer>> call(
                InputSplit inputSplit, Iterator<Tuple2<LongWritable, Text>> keyValuePairs)
                throws Exception {
            // NOTE: Name of source file contains year-month-day string (yyyymmdd),
            // which will be prepended to the first two tokenized strings in each 
            // inputted record [domain code + webpage extension] to form outputtedKey

            final String sourceFile = ((FileSplit) inputSplit).getPath().getName();
            // Filename contains yearMonthDay metadata.
            String yearMonthDay = sourceFile.substring(10, 18);
            
            while (keyValuePairs.hasNext()) {
                String rawDataEntry = keyValuePairs.next()._2().toString();

                if (PageViewsDailyMapper.rawDataEntryIsValid(rawDataEntry)) {
                    // Raw data entry format is space-delimited:
                    //   [domain code] + [webpage extension] + [pageviews] + [total response size]
                    String[] hourlyRecordComponents = rawDataEntry.split(" ");
                    String yearMonthDayDomainCode = yearMonthDay + hourlyRecordComponents[0];
                }
            }
            return new Iterator<Tuple2<String, Integer>>() {
                @Override
                public boolean hasNext() {
                    return keyValuePairs.hasNext();
                }
                @Override
                public Tuple2<String, Integer> next() {
                    if (!keyValuePairs.hasNext()) {
                        throw new NoSuchElementException(); // adhere to Iterator specification!
                    }
                    String outputtedKey = null;
                    Integer outputtedValue = 0;
                    do {
                        String rawDataEntry = keyValuePairs.next()._2().toString();

                        // Raw data entry format is space-delimited:
                        //   [domain code] + [webpage extension] + [pageviews] + [total response size]
                        if (PageViewsDailyMapper.rawDataEntryIsValid(rawDataEntry)) {
                            String[] hourlyRecordComponents = rawDataEntry.split(" ");
                            String yearMonthDayDomainCode = yearMonthDay + hourlyRecordComponents[0];
                            outputtedKey = yearMonthDayDomainCode + " " + hourlyRecordComponents[1];
                            outputtedValue = Integer.parseInt(hourlyRecordComponents[2]);
                        }
                    } while (outputtedKey == null && keyValuePairs.hasNext());
                    
                    return new Tuple2<>(outputtedKey, outputtedValue);
                }
            };
       }
    }
}
