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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Layout of raw data is explained here:
 * https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
 * 
 * @author Daniel Vimont
 */
public class SparkDriver {
    
    // private static int displayCount = 0;
    private static final int POPULAR_PAGES_LIMIT = 500;
    private static final String DISCARD_INDICATOR = "&$";
    private static final DailyMapper DAILY_MAPPER = new DailyMapper();
    private static final WeeklyMapper WEEKLY_MAPPER = new WeeklyMapper();
    private static final MonthlyMapper MONTHLY_MAPPER = new MonthlyMapper();
    private static final YearlyMapper YEARLY_MAPPER = new YearlyMapper();
    // private static HashPartitioner HASH_PARTITIONER;
    public static final String VALUE_ARRAY_OPEN_TAG = "[&[";
    public static final String VALUE_ARRAY_CLOSE_TAG = "]&]";
    public static final String VALUE_ARRAY_DELIMITER = "\n"; // line-feed delimiter mirrors original raw-data delimiter
    private static StorageLevel MASTER_PERSISTENCE_OPTION = StorageLevel.MEMORY_AND_DISK();
    public enum COUNTERS { GOOD, BAD, CONTAINS_TABS, CONTAINS_QUOTES, NONINTEGER_COUNT_OF_VIEWS }
    
    public static void main( String[] args ) throws Exception {
        if (args.length < 7) {
          System.err.println(
                  "Usage: SparkDriver <hdfs-master url> <storage-level> <input path> "
                          + "<daily output path> <weekly output path> "
                          + "<monthly output path> <yearly output path>");
          System.exit(-1);
        }
        String hdfsNamenode = args[0];
        if (args[1].toUpperCase().equals("MEMDISK")) {
            MASTER_PERSISTENCE_OPTION = StorageLevel.MEMORY_AND_DISK();
        } else if (args[1].toUpperCase().equals("DISK")) {
            MASTER_PERSISTENCE_OPTION = StorageLevel.DISK_ONLY();
        }
        String inputHdfsFile = args[2];
        String outputDailyHdfsFile = args[3];
        String outputWeeklyHdfsFile = args[4];
        String outputMonthlyHdfsFile = args[5];
        String outputYearlyHdfsFile = args[6];
        
        System.out.println("Commencing DAILY processing");
        SparkConf conf = new SparkConf().setAppName("WikimediaPageViewsProcessing");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = 
             (JavaNewHadoopRDD) sc.newAPIHadoopFile(hdfsNamenode +
                        inputHdfsFile,  // e.g. "test/raw_files", 
                TextInputFormat.class,  // format of the inputted file data
                LongWritable.class,     // key class
                Text.class,             // value class -- raw data
                new Configuration()     // hadoop config
        );
        JavaPairRDD<String, Integer> pageViewsDaily =
                hadoopRDD.mapPartitionsWithInputSplit(DAILY_MAPPER, true)
                        .mapToPair(tuple -> tuple)
                        // key is [yearMonthDayDomainCode + " " + webpageExtension]
                        .reduceByKey((a, b) -> a + b)  // reduce to count of daily views
                        .filter(tuple -> tuple._2() > 100) // filter out pages w/ small daily-views
                ;
        
        // NOTE: the logic to create each of the *PagesByPopularity RDDs below is
        //   quite similar, and arguably should be modularized to remove redundancy.
        //   Currently, two considerations override that: (1) the wish to keep
        //   the logic as "readable" as possible by those reviewing this code, and 
        //   (2) the fact that this code is in "alpha" state, and as it evolves
        //   into a more production-ready state, unique modifications may need to be
        //   made to either daily, monthly, or weekly processing.
        JavaPairRDD<String, String> dailyPagesByPopularity =
                pageViewsDaily
                        .mapToPair(
                            // new key is yyyymmddnnnnnnnnn, where nnnnnnnnn is views
                            //   key,value example -->> (20160929000001871863,en Main_Page)
                            tuple -> new Tuple2<>(
                                    tuple._1().substring(0, 8) + String.format("%012d", tuple._2()), tuple._1().substring(8)))
                        .sortByKey(false)
                        .mapToPair(new DiscardMapper(8))
                        // each partition will retain only its most popular!
                        .filter(tuple -> (!tuple._2().startsWith(DISCARD_INDICATOR)))
                        .mapToPair(   // new key is yyyymmdd (day)
                            tuple -> new Tuple2<>(
                                    tuple._1().substring(0, 8), tuple._2() + tuple._1().substring(8)))
                        .groupByKey() // YES, groupByKey for denormalization!!
                        .mapToPair(new JsonMapper())
                ;
        dailyPagesByPopularity.saveAsTextFile(hdfsNamenode + outputDailyHdfsFile);

        System.out.println("Commencing MONTHLY processing");
        JavaPairRDD<String, Integer> pageViewsMonthly = 
                pageViewsDaily.mapToPair(MONTHLY_MAPPER)
                        .reduceByKey((a, b) -> a + b);
        
        JavaPairRDD<String, String> monthlyPagesByPopularity =
                pageViewsMonthly
                        .filter(tuple -> tuple._2() > 100) // cull out low-ballers
                        .mapToPair(
                            // new key is yyyymmnnnnnnnnn, where nnnnnnnnn is views
                            //   key,value example -->> (20160929000001871863,20160929en Main_Page)
                            tuple -> new Tuple2<>(
                                    tuple._1().substring(0, 6) + String.format("%012d", tuple._2()), tuple._1().substring(6)))
                        .sortByKey(false)
                        .mapToPair(new DiscardMapper(6))
                        .filter(tuple -> (!tuple._2().startsWith(DISCARD_INDICATOR)))
                        // investigate collapsing the next two (or three) steps into a single #reduceByKey step
                        .mapToPair(   // new key is yyyymm
                            tuple -> new Tuple2<String, String>(
                                    tuple._1().substring(0, 6), tuple._2() + tuple._1().substring(6)))
                        .groupByKey()
                        .mapToPair(new JsonMapper())
                ;
        monthlyPagesByPopularity.saveAsTextFile(hdfsNamenode + outputMonthlyHdfsFile);
        
        System.out.println("Commencing YEARLY processing");
        JavaPairRDD<String, Integer> pageViewsYearly = 
                pageViewsMonthly.mapToPair(YEARLY_MAPPER)
                        .reduceByKey((a, b) -> a + b);
        
        JavaPairRDD<String, String> yearlyPagesByPopularity =
                pageViewsYearly
                        .filter(tuple -> tuple._2() > 100) // cull out low-ballers
                        .mapToPair(
                            // new key is yyyynnnnnnnnnnnn, where nnnnnnnnnnnn is views
                            //   key,value example -->> (20160929000001871863,20160929en Main_Page)
                            tuple -> new Tuple2<>(
                                    tuple._1().substring(0, 4) + String.format("%012d", tuple._2()), tuple._1().substring(4)))
                        .sortByKey(false)
                        .mapToPair(new DiscardMapper(4))
                        .filter(tuple -> (!tuple._2().startsWith(DISCARD_INDICATOR)))
                        .mapToPair(  // new key is yyyy
                            tuple -> new Tuple2<>(
                                    tuple._1().substring(0, 4), tuple._2() + tuple._1().substring(4)))
                        .groupByKey()
                        .mapToPair(new JsonMapper())
                ;
        yearlyPagesByPopularity.saveAsTextFile(hdfsNamenode + outputYearlyHdfsFile);
        
        // TO DO: compute and output pageviewsByWebpage
        // JavaPairRDD<String, String> pageviewsByWebpage;
        //    key is [domainCode + " " + webpageExtension]
        //    value is JSON array of all (date & viewCount-for-that-date) for the webpage
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
            String yearMonthDay = sourceFile.substring(10, 18);
            
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
                        if (rawDataEntryIsValid(
                                sourceFile, rawDataEntry, true)) {
                            String[] hourlyRecordComponents = rawDataEntry.split(" ");
                            String yearMonthDayDomainCode = yearMonthDay + hourlyRecordComponents[0];
                            // insert JSON escape char for all double-quotes in URL extensions
                            String webpageExtension = hourlyRecordComponents[1].replaceAll("\"", "\\\\\"");
                            outputtedKey = yearMonthDayDomainCode + " " + webpageExtension;
                            outputtedValue = Integer.parseInt(hourlyRecordComponents[2]);
                        }
                    } while (outputtedKey == null && keyValuePairs.hasNext());
                    
                    return new Tuple2<>(outputtedKey, outputtedValue);
                }
            };
       }
    }
    
    static class DiscardMapper
            implements PairFunction<Tuple2<String, String>, String, String> {
        int counter = 0;
        String currentDayKey = "";
        final int timestampLength;
        
        public DiscardMapper(int timestampLength) {
            this.timestampLength = timestampLength;
        }
        
        @Override
        public Tuple2<String, String> call(Tuple2<String, String> keyValuePair)
                throws Exception {
            if (!currentDayKey.equals(keyValuePair._1().substring(0, timestampLength))) {
                currentDayKey = keyValuePair._1().substring(0, timestampLength);
                counter = 0;
            }
            String outputtedValue;
            if (++counter > POPULAR_PAGES_LIMIT) {
                outputtedValue = DISCARD_INDICATOR + keyValuePair._2();
            } else {
                outputtedValue = keyValuePair._2();
            }
            return new Tuple2(keyValuePair._1(), outputtedValue);
        }
    }
    
    static class JsonMapper
            implements PairFunction<Tuple2<String, Iterable<String>>, String, String> { 
        
        private TreeMap<String, String> itemMap;
        
        @Override
        public Tuple2<String, String> call(Tuple2<String, Iterable<String>> keyValuePair)
                throws Exception {
            // Assemble TreeMap with most popular items up to size limit of POPULAR_PAGES_LIMIT
            //   Note that #groupByKey necessitates this because it can destroy the ordering from the sort
            itemMap = new TreeMap<>();
            long itemCount = 0L;
            for (String value : keyValuePair._2()) {
                ++itemCount;
                // addEntry is passed (key == viewCount, value == complete record)
                addEntry(value.substring(value.length() - 12), value);
            }
            // System.out.println("#### Number of items originally passed to JSONMapper in Iterable: " + itemCount);
            TreeMap<String, String> descendingMap = new TreeMap(Collections.reverseOrder());
            descendingMap.putAll(itemMap);
            
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(SimpleJson.OBJECT_OPEN);
            stringBuilder.append(SimpleJson.nameValuePair("interval", keyValuePair._1()));
            stringBuilder.append(",");
            stringBuilder.append("\"topPages\":");
            stringBuilder.append(SimpleJson.ARRAY_OPEN);
            boolean pastFirstValue = false;
            int rank = 0;
            for (Entry<String, String> entry : descendingMap.entrySet()) {
                if (!pastFirstValue) {
                    pastFirstValue = true;
                } else {
                    stringBuilder.append(",");
                }
                stringBuilder.append(SimpleJson.OBJECT_OPEN);
                String pageId = entry.getValue()
                        .substring(0, entry.getValue().length() - 12);
                String[] tokens = entry.getValue().split(" ");
                String pageUrlExtension = tokens[1].substring(0, tokens[1].length() - 12);
                String viewsWithLeadingZeroes = tokens[1].substring(tokens[1].length() - 12);
                String views = Integer.valueOf(viewsWithLeadingZeroes).toString();
                
                stringBuilder.append(SimpleJson.nameValuePair(
                        "rank", String.valueOf(++rank)));
                stringBuilder.append(",");
                stringBuilder.append(SimpleJson.nameValuePair(
                        "pageId", pageId));
                stringBuilder.append(",");
                stringBuilder.append(SimpleJson.nameValuePair(
                        "url", "https://en.wikipedia.org/wiki/" + pageUrlExtension));
                stringBuilder.append(",");
                stringBuilder.append(SimpleJson.nameValuePair(
                        "topic", pageUrlExtension.replaceAll("_", " ")));
                stringBuilder.append(",");
                stringBuilder.append(SimpleJson.nameValuePair(
                        "views", views));
                stringBuilder.append(SimpleJson.OBJECT_CLOSE);
            }
            stringBuilder.append(SimpleJson.ARRAY_CLOSE);
            stringBuilder.append(SimpleJson.OBJECT_CLOSE);
            return new Tuple2(keyValuePair._1(), stringBuilder.toString());
        }
        
        private void addEntry(String key, String value) {
          if (itemMap.size() <  POPULAR_PAGES_LIMIT) {
            itemMap.put(key, value);
          } else {
            if (key.compareTo(itemMap.firstEntry().getKey()) > 0) {
              itemMap.pollFirstEntry(); // remove earliest entry
              itemMap.put(key, value);
            }
          }
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
    
    public static boolean rawDataEntryIsValid(
            String sourceFile, String rawDataEntry, boolean verboseMode) {
        return rawDataEntryIsValid(null, sourceFile, 0, rawDataEntry, true);
    }
    
    public static boolean rawDataEntryIsValid(Mapper.Context context, String sourceFile,
            long key, String rawDataEntry, boolean verboseMode) {
        // 2017-09-20 decided to initially work only with English Wikimedia pages
        // 2017-09-24 -- added space after "en" for only wikiPEDIA
        //  see https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
        if (!rawDataEntry.startsWith("en ")) {
            return false;
        }
        // Curious page with just hyphen as URL extension should be filtered out!
        if (rawDataEntry.length() > 5 && rawDataEntry.substring(3, 5).equals("- ")) {
            return false;
        }
        if (rawDataEntry.contains("\t")) {
            if (verboseMode) {
                System.out.println(
                        "** Encountered invalid entry CONTAINING TAB(s) in file <" + sourceFile
                        + ">, position <" + key + "> -- raw data entry: <" + rawDataEntry + ">");
            }
            if (context != null) {
                context.getCounter(COUNTERS.CONTAINS_TABS).increment(1L);
            }
            return false;
        }
        String[] parsedData = rawDataEntry.split(" ");
        if (parsedData.length != 4) {
            if (verboseMode) {
                System.out.println("** Encountered invalid entry WITH <" + parsedData.length
                        + "> SPACE-DELIMITED ELEMENTS (expected 4) in file <" + sourceFile
                        + ">, position <" + key + "> -- raw data entry: <" + rawDataEntry + ">");
            }
            return false;
        }
        if (!isIntegerValue(parsedData[2])) {
            if (verboseMode) {
                System.out.println("** Encountered invalid raw-data line WITH INVALID COUNT_VIEWS"
                        + "VALUE OF <" + parsedData[2] + "> in file <" + sourceFile
                        + ">, position <" + key + "> -- raw data entry: <" + rawDataEntry + ">");
            }
            if (context != null) {
                context.getCounter(COUNTERS.NONINTEGER_COUNT_OF_VIEWS).increment(1L);
            }
            return false;
        }
        return true;
    }

    private static boolean isIntegerValue(String validationString) {
        if (validationString == null || validationString.isEmpty()) {
            return false;
        } else {
            for (char c : validationString.toCharArray()) {
                if (!Character.isDigit(c)) {
                    return false;
                }
            }
        }
        return true;
    }
}