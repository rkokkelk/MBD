/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.zzp;

import java.io.IOException;
import java.util.Map;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.json.*;

public class DateCounter {

  public static class DateMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private Text date  = new Text();
    private IntWritable count = new IntWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String input = value.toString();
      String[] inputs = input.split(",");

      long start_date = Long.parseLong(inputs[1]);
      long end_date = Long.parseLong(inputs[2]);

      while(start_date < end_date){
        Date tmp_date = new Date(start_date);
        String result = Helper.formatDate(tmp_date);
        date.set(result);
        count.set(1);
        context.write(date,count);
        start_date += 86400; // increase epoch by 1 day
      }
    }
  }
  
  public static class DateReducer
       extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      int total = 0;
      Text date = new Text();
      IntWritable sum = new IntWritable();

      for (IntWritable tmpValue : values) {
        total += tmpValue.get();
      }

      sum.set(total);
      context.write(key, sum);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: DateCounter <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Date Counter");
    job.setJarByClass(DateCounter.class);
    job.setMapperClass(DateMapper.class);
    job.setReducerClass(DateReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
