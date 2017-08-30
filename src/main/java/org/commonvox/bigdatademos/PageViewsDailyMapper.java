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

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author Daniel Vimont
 */
public class PageViewsDailyMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String sourceFile = ((FileSplit)context.getInputSplit()).getPath().getName();
        String yearMonthDay = sourceFile.substring(10, 18);
        String rawDataEntry = value.toString();
        String[] hourlyRecordComponents = rawDataEntry.split(" ");

        if (rawDataEntryIsValid(context, sourceFile, key, rawDataEntry, true)) {
            String yearMonthDayDomainCode = yearMonthDay + hourlyRecordComponents[0];

            context.write(new Text(yearMonthDayDomainCode + " " + hourlyRecordComponents[1]),
                    new IntWritable(Integer.parseInt(hourlyRecordComponents[2])));
            context.getCounter(PageViewsDaily.COUNTERS.GOOD).increment(1L);
        } else {
            context.getCounter(PageViewsDaily.COUNTERS.BAD).increment(1L);
        }
    }

    private static boolean rawDataEntryIsValid(Context context, String sourceFile,
            LongWritable key, String rawDataEntry, boolean verboseMode) {
        if (rawDataEntry.contains("\t")) {
            if (verboseMode) {
                System.out.println(
                        "** Encountered invalid entry CONTAINING TABS in file <" + sourceFile
                        + ">, position <" + key + "> -- raw data entry: <" + rawDataEntry + ">");
            }
            context.getCounter(PageViewsDaily.COUNTERS.CONTAINS_TABS).increment(1L);
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
            context.getCounter(PageViewsDaily.COUNTERS.NONINTEGER_COUNT_OF_VIEWS).increment(1L);
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
