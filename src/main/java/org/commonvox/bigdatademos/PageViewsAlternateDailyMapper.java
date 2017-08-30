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
public class PageViewsAlternateDailyMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /* following logic works with consolidated version of data outputted by WikiMediaFileConsolidator */
    String[] hourlyRecordComponents = value.toString().split(" ");
    String yearMonthDayDomainCode = hourlyRecordComponents[0].substring(0, 8) +
            hourlyRecordComponents[0].substring(10);

    // Note that hourlyRecordComponents[1] is the webpage title (Domain Code + webpage title uniquely identifies webpage),
    //   and hourlyRecordComponents[2] is the hourly count of views for the webpage.
    context.write(new Text(yearMonthDayDomainCode + " " + hourlyRecordComponents[1]),
            new IntWritable(Integer.parseInt(hourlyRecordComponents[2])));
  }
}
