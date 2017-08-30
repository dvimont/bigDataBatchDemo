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

/**
 *
 * @author Daniel Vimont
 */
public class PageViewsYearlyMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Mapper.Context context)
      throws IOException, InterruptedException {

    String[] monthlyKeyValuePair = value.toString().split("\t"); // tab-delimited
    String[] monthlyKeyComponents = monthlyKeyValuePair[0].split(" ");
    String yearDomainCode = monthlyKeyComponents[0].substring(0, 4) + monthlyKeyComponents[0].substring(6);

    // Note that keyComponents[1] is the webpage title (DomainCode + webpage title uniquely identifies webpage),
    //   and monthlyKeyValuePair[1] is the monthly count of views for the webpage.
    context.write(new Text(yearDomainCode + " " + monthlyKeyComponents[1]),
            new IntWritable(Integer.parseInt(monthlyKeyValuePair[1])));
  }
}
