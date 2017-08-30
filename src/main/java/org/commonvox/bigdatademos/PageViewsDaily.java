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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce job to input hourly pageview records and output daily pageview records
 * 
 * @author Daniel Vimont
 */
public class PageViewsDaily {
    public enum COUNTERS { GOOD, BAD, CONTAINS_TABS, NONINTEGER_COUNT_OF_VIEWS }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
          System.err.println("Usage: PageViewsDaily <input path> <output path>");
          System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(PageViewsDaily.class);
        job.setJobName("DeriveDailyPageViews");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PageViewsDailyMapper.class);
        job.setReducerClass(PageViewsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
