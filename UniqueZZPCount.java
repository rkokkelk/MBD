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

package nl.utwente.bigdata;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

public class UniqueZZPCount {

  public static class CountMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text dateString  = new Text();
    private Text UserIdString  = new Text();
    private JSONParser parser = new JSONParser();
    private JSONObject userObject = new JSONObject();
    private Map tweet;
    private Text createdAt = new Text();
    private Text createYear = new Text();
    private Text createMonth = new Text();
    private Text createDay = new Text();
    private Text tweetText = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        try {
            tweet = (Map<String, Object>) parser.parse(value.toString());
          }
          catch (ClassCastException e) {  
            return; // do nothing (we might log this)
          }
          catch (org.json.simple.parser.ParseException e) {  
            return; // do nothing 
          }
        

        createdAt.set((String) tweet.get("created_at"));
        createYear.set(createdAt.substring(createdAt.lastIndexOf(" ")+1));
        createMonth.set(createdAt.split("\\s+")[1]);
        createDay.set(createdAt.split("\\s+")[2]);

        userObject = (JSONObject) tweet.get("user");
        UserIdString.set((String) user.get("id_str"));

        tweetText.set((String) tweet.get('text'));

        // Find words
        if (tweetText.toLowerCase().indexOf("zzp") != -1 ) {
         dateString.set(createYear.concat(createMonth.concat(createDay))));
         context.write(dateString, UserIdString);
        }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Text values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(CountMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}