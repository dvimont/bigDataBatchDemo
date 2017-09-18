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

import java.time.LocalDate;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author Daniel Vimont
 */
public class SparkDriverToRiak {
    
//    private static final String hdfsNamenode =
//            "hdfs://ec2-54-164-189-32.compute-1.amazonaws.com:50070/";

//    private static int displayCount = 0;
    private static final DailyMapper DAILY_MAPPER = new DailyMapper();
    private static final WeeklyMapper WEEKLY_MAPPER = new WeeklyMapper();
    private static final MonthlyMapper MONTHLY_MAPPER = new MonthlyMapper();
    private static final YearlyMapper YEARLY_MAPPER = new YearlyMapper();
    private static HashPartitioner HASH_PARTITIONER;
    
    public static void main( String[] args ) throws Exception {
        if (args.length < 7) {
          System.err.println(
                  "Usage: PageViewsDaily <hdfs-master url> <partition-count> <input path> "
                          + "<daily output path> <weekly output path> "
                          + "<monthly output path> <yearly output path>");
          System.exit(-1);
        }
        String hdfsNamenode = args[0];
        HASH_PARTITIONER = new HashPartitioner(Integer.valueOf(args[1]));
        String inputHdfsFile = args[2];
        String outputDailyHdfsFile = args[3];
        String outputWeeklyHdfsFile = args[4];
        String outputMonthlyHdfsFile = args[5];
        String outputYearlyHdfsFile = args[6];
        
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
        JavaPairRDD<String, Integer> pageViewsDaily =
                hadoopRDD.mapPartitionsWithInputSplit(DAILY_MAPPER, true)
                        .mapToPair(tuple -> tuple)
                        .partitionBy(HASH_PARTITIONER)
                        .persist(StorageLevel.MEMORY_AND_DISK())
                        .reduceByKey((a, b) -> a + b);
        
//        pageViewsDaily.saveAsTextFile(hdfsNamenode + outputDailyHdfsFile); // "test/pageviews.daily");

        JavaPairRDD<String, Iterable<String>> dailyPagesByPopularity =
                pageViewsDaily.mapToPair(
                        // new key is yyyymmddnnnnnnnnn, where nnnnnnnnn is views
                        //   key,value example -->> (20160929000001871863,20160929en Main_Page)
                        tuple -> new Tuple2<String, String>(
                                tuple._1().substring(0, 8) + String.format("%12d", tuple._2()), tuple._1()))
                        .sortByKey(false)
                        
                        .mapToPair(
                                // new key is yyyymmdd (day) -- BIG QUESTION: will sorted order be maintained?
                                tuple -> new Tuple2<String, String>(
                                        tuple._1().substring(0, 8), tuple._2() + tuple._1().substring(8)))
                        .groupByKey();
        
        dailyPagesByPopularity.saveAsTextFile(hdfsNamenode + outputDailyHdfsFile);

        
//        JavaPairRDD<String, Integer> pageViewsWeekly = 
//                pageViewsDaily.mapToPair(WEEKLY_MAPPER)
//                        .partitionBy(HASH_PARTITIONER)
//                        .reduceByKey((a, b) -> a + b);
//        
//        pageViewsWeekly.saveAsTextFile(hdfsNamenode + outputWeeklyHdfsFile);
//        
//        JavaPairRDD<String, Integer> pageViewsMonthly = 
//                pageViewsDaily.mapToPair(MONTHLY_MAPPER)
//                        .partitionBy(HASH_PARTITIONER)
//                        .persist(StorageLevel.MEMORY_AND_DISK())
//                        .reduceByKey((a, b) -> a + b);
//        
//        pageViewsMonthly.saveAsTextFile(hdfsNamenode + outputMonthlyHdfsFile);
//        
//        JavaPairRDD<String, Integer> pageViewsYearly = 
//                pageViewsMonthly.mapToPair(YEARLY_MAPPER)
//                        .partitionBy(HASH_PARTITIONER)
//                        .reduceByKey((a, b) -> a + b);
//        
//        pageViewsYearly.saveAsTextFile(hdfsNamenode + outputYearlyHdfsFile);
        
    }
    
    static class DailyMapper implements Function2<InputSplit,
            Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, Integer>>> {
        @Override
        public Iterator<Tuple2<String, Integer>> call(
                InputSplit inputSplit, Iterator<Tuple2<LongWritable, Text>> keyValuePairs)
                throws Exception {

//            System.out.println("DailyMapper processing is commencing!");

            // NOTE: Name of source file contains year-month-day string (yyyymmdd),
            // which will be prepended to the first two tokenized strings in each 
            // inputted record [domain code + webpage extension] to form outputtedKey
            final String sourceFile = ((FileSplit) inputSplit).getPath().getName();
            // Filename contains yearMonthDay metadata.
            String yearMonthDay = sourceFile.substring(10, 18);
            
//            System.out.println("sourceFile is: " + sourceFile);
//            System.out.println("yearMonthDay value is: " + yearMonthDay);
            
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
//                        if (++displayCount < 20) {
//                            System.out.println("-- rawDataEntry: " + rawDataEntry);
//                        }

                        // Raw data entry format is space-delimited:
                        //   [domain code] + [webpage extension] + [pageviews] + [total response size]
                        if (PageViewsDailyMapper.rawDataEntryIsValid(
                                sourceFile, rawDataEntry, true)) {
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
    
    static class WeeklyMapper
             implements PairFunction<Tuple2<String, Integer>, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(Tuple2<String, Integer> keyValuePair)
                throws Exception {
            String[] dailyKeyComponents = keyValuePair._1().split(" ");
            // Note that the common key for all days in a given week is based on nearest preceding Sunday!!
            LocalDate localDate = LocalDate.of(
                    Integer.parseInt(dailyKeyComponents[0].substring(0, 4)),
                    Integer.parseInt(dailyKeyComponents[0].substring(4, 6)),
                    Integer.parseInt(dailyKeyComponents[0].substring(6, 8)));
            int sundayOffset = localDate.getDayOfWeek().getValue() % 7;
            String nearestPrecedingSunday = localDate.minusDays(sundayOffset).toString();
            String yearMonthSundayDomainCode = nearestPrecedingSunday.substring(0, 4) +
                    nearestPrecedingSunday.substring(5, 7) +
                    nearestPrecedingSunday.substring(8, 10) +
                    dailyKeyComponents[0].substring(8);

            // Note that dailyKeyComponents[1] is the webpage title (Domain Code + webpage title uniquely identifies webpage),
            //   and keyValuePair._2 is the daily count of views for the webpage.
            return new Tuple2(yearMonthSundayDomainCode + " " + dailyKeyComponents[1], keyValuePair._2());
        }
     }
    
    static class MonthlyMapper
             implements PairFunction<Tuple2<String, Integer>, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(Tuple2<String, Integer> keyValuePair)
                throws Exception {
            String[] dailyKeyComponents = keyValuePair._1().split(" ");
            String yearMonthDomainCode =
                    dailyKeyComponents[0].substring(0, 6) + dailyKeyComponents[0].substring(8);

            // Note that keyComponents[1] is the webpage title 
            //   (Domain Code + webpage title uniquely identifies webpage),
            //   and keyValuePair._2 is the daily count of views for the webpage.
            return new Tuple2(yearMonthDomainCode + " " + dailyKeyComponents[1], keyValuePair._2());
        }
     }
    
    static class YearlyMapper
             implements PairFunction<Tuple2<String, Integer>, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(Tuple2<String, Integer> keyValuePair)
                throws Exception {
            String[] monthlyKeyComponents = keyValuePair._1().split(" ");
            String yearDomainCode = monthlyKeyComponents[0].substring(0, 4) +
                    monthlyKeyComponents[0].substring(6);

            // Note that keyComponents[1] is the webpage title (DomainCode + webpage title uniquely identifies webpage),
            //   and keyValuePair._2 is the monthly count of views for the webpage.
            return new Tuple2(yearDomainCode + " " + monthlyKeyComponents[1], keyValuePair._2());
        }
     }
}
