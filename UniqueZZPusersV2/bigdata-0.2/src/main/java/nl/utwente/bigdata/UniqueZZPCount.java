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
  private static String[] searchMentions = {"StZZPNederland","ZPnetwerk"};
  private static String[] searchWords = {"zzp","freelancer"," var ", " var-"}; // Add spaces otherwise FP are found
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
            for (String searchWord: searchWords){
              if (tweetText.indexOf(searchWord) != -1 ) {
                polarity += 1;  
              }
            }

            // Search for mentions
            for (String searchMention : searchMentions){
              if (tweetText.indexOf(searchMention) != -1 ) {
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
    private  Map<String,Integer> pValue=new HashMap<String,Integer>();  

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //ArrayList<String> listOfUsers = new ArrayList<String>();
      //int sum = 0;
      //for (Text val : values){
        //if(!listOfUsers.contains(val.toString())){
        //  sum += 1;
        //  listOfUsers.add(val.toString());
        //}
        //}
        
        // Add the data
        pValue.put("2011Jan",19);
        pValue.put("2011Feb",20);
        pValue.put("2011Mar",22);
        pValue.put("2011Apr",20);
        pValue.put("2011May",21);
        pValue.put("2011Jun",23);
        pValue.put("2011Jul",24);
        pValue.put("2011Aug",24);
        pValue.put("2011Sep",22);
        pValue.put("2011Oct",23);
        pValue.put("2011Nov",22);
        pValue.put("2011Dec",23);
        pValue.put("2012Jan",25);
        pValue.put("2012Feb",23);
        pValue.put("2012Mar",25);
        pValue.put("2012Apr",25);
        pValue.put("2012May",27);
        pValue.put("2012Jun",26);
        pValue.put("2012Jul",28);
        pValue.put("2012Aug",28);
        pValue.put("2012Sep",26);
        pValue.put("2012Oct",30);
        pValue.put("2012Nov",29);
        pValue.put("2012Dec",32);
        pValue.put("2013Jan",32);
        pValue.put("2013Feb",29);
        pValue.put("2013Mar",31);
        pValue.put("2013Apr",29);
        pValue.put("2013May",31);
        pValue.put("2013Jun",27);
        pValue.put("2013Jul",30);
        pValue.put("2013Aug",28);
        pValue.put("2013Sep",26);
        pValue.put("2013Oct",25);
        pValue.put("2013Nov",23);
        pValue.put("2013Dec",21);
        pValue.put("2014Jan",22);
        pValue.put("2014Feb",19);
        pValue.put("2014Mar",21);
        pValue.put("2014Apr",19);
        pValue.put("2014May",14);
        pValue.put("2014Jun",11);
        pValue.put("2014Jul",11);
        pValue.put("2014Aug",10);
        pValue.put("2014Sep",9);
        pValue.put("2014Oct",10);
        pValue.put("2014Nov",10);
        pValue.put("2014Dec",10);
        pValue.put("2015Jan",11);
        pValue.put("2015Feb",9);
        pValue.put("2015Mar",12);
        pValue.put("2015Apr",10);
        pValue.put("2015May",10);
        pValue.put("2015Jun",9);
        pValue.put("2015Jul",9);
        pValue.put("2015Aug",9);
        pValue.put("2015Sep",10);
        pValue.put("2015Oct",10);
        pValue.put("2015Nov",1);

        int polarityThreshold = 1000;
        if (pValue.containsKey(key.toString())){
          polarityThreshold = (int) Math.floor(0.4*((double) pValue.get(key.toString())));
        }


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
        if ((int) pair.getValue() > polarityThreshold){
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