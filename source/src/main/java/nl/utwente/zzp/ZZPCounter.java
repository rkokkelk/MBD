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
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ZZPCounter {

  private static String[] searchMentions = {"StZZPNederland","ZPnetwerk"};
  private static String[] searchWords = {"zzp","freelancer"," var ", " var-"}; // Add spaces otherwise FP are found
  private static String[] blackList = {"vacature"};

  public static class CounterMapper extends Mapper<Object, Text, Text, Text> {
    
    private Text idString  = new Text();
    private Text json = new Text();
    private JSONObject tweet;
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String tweetText = "";
      Date createdAt = null;
      //JSONObject user = null;
      JSONObject entities = null;
      JSONArray mentions = null;
      JSONArray hashtags = null;

      try {
        tweet = new JSONObject(value.toString());
        tweetText = tweet.getString("text");
        //user = tweet.getJSONObject("user");
        entities = tweet.getJSONObject("entities");
        mentions = entities.getJSONArray("user_mentions");
        hashtags = entities.getJSONArray("hashtags");
        createdAt = Helper.parseDate(tweet.getString("created_at"));
      } catch(JSONException je) {
        System.err.println("Error parsing value: "+je.getMessage());
        return;
      }

      // Search hashtags
      for (Object obj: hashtags) {
        JSONObject hashtag = (JSONObject) obj;
        // Search for blacklist, if so then return
        for (String black : blackList) {
          if (black.equals(hashtag.getString("text"))) {
            return;
          }
        }
        for (String searchWord : searchWords) {
          if (searchWord.equals(hashtag.getString("text"))) {
            Helper.increaseCounter(tweet);
          }
        }
      }

      // Search for mentions
      for (Object obj : mentions) {
        JSONObject mention = (JSONObject) obj;
        for (String searchMention : searchMentions){
          if (searchMention.equals(mention.getString("screen_name"))) {
            Helper.increaseCounter(tweet);
            break;
          }
        }
      }

      // Search for words
      for (String searchWord : searchWords) {
        if (tweetText.indexOf(searchWord) != -1 ) {
          Helper.increaseCounter(tweet);	
        }
      }

      // Only write tweets which have higher polarity
      if (Helper.hasPolarity(tweet)) {
        idString.set(Helper.formatDate(createdAt));
        json.set(tweet.toString());
        context.write(idString, json);
      }
    }
  }
  
  public static class CounterReducer extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      HashMap<String, Integer> users = new HashMap<String, Integer>();
      String userId = "";
      JSONObject tweet;
      IntWritable value = new IntWritable();
      int sum_polarity = 0;

      for (Text tmpValue : values) {
        int polarity = 0;

        try {
          tweet = new JSONObject(tmpValue.toString());
          JSONObject user = tweet.getJSONObject("user");
          userId = user.getString("id_str");
          polarity = tweet.getInt(Helper.KEY);
        } catch(JSONException je) {
          System.err.println("Error parsing: " + tmpValue.toString());
          return;
        }

        if (users.containsKey(userId)) {
          polarity += users.get(userId);
        }
        users.put(userId,polarity);
      }

      for(String tmpKey : users.keySet()) {
        if(Helper.isZZP(users.get(tmpKey))) {
          sum_polarity++;
        }
      }
      
      value.set(sum_polarity);
      context.write(key, value);
    }
  }
}
