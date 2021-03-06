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
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

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
  private static String[] blackList = {"vacature"};

  public static class CountMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text dateString  = new Text();
    private Text UserIdString  = new Text();
    private JSONParser parser = new JSONParser();
    private JSONObject userObject = new JSONObject();
    private Map tweet;
    private String createdAt = new String();
    private String createYear = new String();
    private String createMonth = new String();
    private String tweetText = new String();
    private int polarity;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      try {
          tweet = (Map<String, Object>) parser.parse(value.toString());
          polarity = 0;

          createdAt = (String) tweet.get("created_at");
          if (createdAt != null){
            createYear = createdAt.substring(createdAt.lastIndexOf(" ")+1);
            createMonth = createdAt.split("\\s+")[1];

            userObject = (JSONObject) tweet.get("user");
            UserIdString.set((String) userObject.get("id_str"));

            tweetText = (String) tweet.get("text");


            for (String black : blackList){
              if (tweetText.indexOf(black) != -1)
                  return;
            }

            // Search for words
            if (tweetText.toLowerCase().indexOf("zzp") != -1 || tweetText.toLowerCase().indexOf("freelancer") != -1 || tweetText.toLowerCase().indexOf("zelfstandige") != -1) {
              if (tweetText.toLowerCase().indexOf("eerste opdracht") != -1 ) {
                polarity += 1;
              }
              if (tweetText.toLowerCase().indexOf("tweede opdracht") != -1 ) {
                polarity += 1;
              }
              if (tweetText.toLowerCase().indexOf("eerste werkdag") != -1 ) {
                polarity += 1;
              }
              if (tweetText.toLowerCase().indexOf("ingeschreven") != -1 ) {
                if (tweetText.toLowerCase().indexOf("kvk") != -1 ) {
                  polarity += 1;
                }
              }
              if (tweetText.toLowerCase().indexOf("nieuwe opdrachtgever") != -1 ) {
                polarity += 1;
              }
            }

            if (tweetText.toLowerCase().indexOf("ik ben zzp") != -1 ) {
              polarity += 1;
            }
            if (tweetText.toLowerCase().indexOf("ik ben freelancer") != -1 ) {
              polarity += 1;
            }
            if (tweetText.toLowerCase().indexOf("ik ben zelfstandige") != -1 ) {
              polarity += 1;
            }

            if (tweetText.toLowerCase().indexOf("var ") != -1 ) {
              if (tweetText.toLowerCase().indexOf("aangevraagd") != -1 ) {
                polarity += 1;
              }
              if (tweetText.toLowerCase().indexOf("gekregen") != -1 ) {
                polarity += 1;
              }
            }

            if (polarity > 0) {
             dateString.set(createYear.concat(createMonth));
             context.write(dateString, UserIdString);
            }
          }
        }
        catch (ClassCastException e) {  
          return; // do nothing (we might log this)
        }
        catch (org.json.simple.parser.ParseException e) {  
          return; // do nothing 
        }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,IntWritable> {
    private IntWritable result = new IntWritable(0);

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      



        int sum = 0;
        Map<String,Integer> polarityCounter=new HashMap<String,Integer>();
        for (Text val : values){
            if(polarityCounter.containsKey(val.toString()))
            {
                polarityCounter.put(val.toString(), polarityCounter.get(val.toString())+1);
            } 
            else
            {
                polarityCounter.put(val.toString(), 1);
            }
       }
      

      Iterator it = polarityCounter.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry pair = (Map.Entry)it.next();
        if ((int) pair.getValue() > 1){
          sum += 1;
          it.remove(); // avoids a ConcurrentModificationException
        }
      }
      if (sum != 0){
        result.set(sum);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "unique zpp count");

    job.setJarByClass(UniqueZZPCount.class);
    job.setMapperClass(CountMapper.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}