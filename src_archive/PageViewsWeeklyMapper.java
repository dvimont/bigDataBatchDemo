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
import java.time.LocalDate;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Daniel Vimont
 */
public class PageViewsWeeklyMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Mapper.Context context)
      throws IOException, InterruptedException {

    String[] dailyKeyValuePair = value.toString().split("\t"); // tab-delimited
    String[] dailyKeyComponents = dailyKeyValuePair[0].split(" ");
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
    //   and dailyKeyValuePair[1] is the daily count of views for the webpage.
    context.write(new Text(yearMonthSundayDomainCode + " " + dailyKeyComponents[1]),
            new IntWritable(Integer.parseInt(dailyKeyValuePair[1])));
  }
}
