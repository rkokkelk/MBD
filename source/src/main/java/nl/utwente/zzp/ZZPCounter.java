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

public class ZZPCounter {

  private static String[] searchMentions = {"StZZPNederland","ZPnetwerk"};
  private static String[] searchWords = {"zzp","freelancer"," var ", " var-"}; // Add spaces otherwise FP are found
  private static String[] blackList = {"vacature"};

  public static class CounterMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text idString  = new Text();
    private Text json = new Text();
    private JSONObject tweet;
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String tweetText = "";
      JSONObject user = null;
      JSONObject entities = null;
      JSONArray mentions = null;
      JSONArray hashtags = null;

      try{
        tweet = new JSONObject(value.toString());
      }catch(JSONException je){
        System.err.println("Error parsing: "+value.toString());
        return;
      }

      try{
        tweetText = tweet.getString("text");
        user = tweet.getJSONObject("user");
        entities = tweet.getJSONObject("entities");
        mentions = entities.getJSONArray("user_mentions");
        hashtags = entities.getJSONArray("hashtags");
        idString.set(user.getString("id_str"));
      }catch(JSONException je){
        System.err.println("Error retrieving value: "+je.getMessage());
        return;
      }

      // Search hashtags
      for (Object obj: hashtags){
        JSONObject hashtag = (JSONObject) obj;
        // Search for blacklist, if so then return
        for (String black : blackList){
          if (black.equals(hashtag.getString("text")))
              return;
        }
        for (String searchWord : searchWords){
          if (searchWord.equals(hashtag.getString("text")))
            Helper.increaseCounter(tweet);
        }
      }

      // Search for mentions
      for (Object obj: mentions){
        JSONObject mention = (JSONObject) obj;
        for (String searchMention : searchMentions){
          if (searchMention.equals(mention.getString("screen_name"))){
            Helper.increaseCounter(tweet);
            break;
          }
        }
      }

      // Search for words
      for (String searchWord: searchWords){
        if (tweetText.indexOf(searchWord) != -1 ) {
					Helper.increaseCounter(tweet);	
				}
			}

      // Only write tweet messages which have higher pollarity
			if(Helper.hasPolarity(tweet)){
        json.set(tweet.toString());
        context.write(idString, json);
      }
    }
  }
  
  public static class CounterReducer
       extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      JSONObject tweet;
      Text value = new Text();
      Date start_date, end_date;
      start_date = end_date = null;
      int sum_polarity = 0;
      boolean set_create_date = false;

      for (Text tmpValue : values) {

        try{
          tweet = new JSONObject(tmpValue.toString());
        }catch(JSONException je){
          System.err.println("Error parsing: "+tmpValue.toString());
          return;
        }

        if(!set_create_date){
          JSONObject user = tweet.getJSONObject("user");
          String date_create = user.getString("created_at");
          start_date = Helper.parseDate(date_create);
          set_create_date = true;
        }

        // Ensure that the latest date matches the last tweet send
        Date tmp = Helper.parseDate(tweet.getString("created_at"));
        end_date = Helper.getLatestDate(end_date, tmp);

        sum_polarity += tweet.getInt(Helper.KEY);
      }

      if(Helper.isZZP(sum_polarity)){
        // Create CSV, Polarity,User_Created,Last_send_tweet
        String result = ""+sum_polarity;
        result += ","+start_date.getTime();
        result += ","+end_date.getTime();
        value.set(result);
        context.write(key, value);
      }
    }
  }
}
