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

  private static String[] searchWords = {"@StZZPNederland","@ZPnetwerk"};

  public static class CounterMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text idString  = new Text();
    private Text json = new Text();
    private JSONObject tweet;
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      tweet = new JSONObject(value.toString());

      idString.set(tweet.getString("id_str"));
      String tweetText = tweet.getString("text");

      for (String searchWord: searchWords){
        if (tweetText.indexOf(searchWord) != -1 ) {
					Helper.increaseCounter(tweet);	
					break;
				}
			}

			json.set(tweet.toString());
      context.write(idString, json);
    }
  }
  
  public static class CounterReducer
       extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text value : values) {
        context.write(key, value);
      }
    }
  }
}
