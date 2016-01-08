import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import nl.utwente.bigdata.TwitterExample;   // Change this line
 
public class Group03Test {
 

  private MapDriver<Object, Text, Text, Text> mapDriver;                          // Change this line
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;                      // Change this line
  private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;  // Change this line
 
  @Before
  public void setUp() {
    TwitterExample.ExampleMapper mapper   = new TwitterExample.ExampleMapper();   // Change this line
    TwitterExample.ExampleReducer reducer = new TwitterExample.ExampleReducer();  // Change this line
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    Object key = new Object();
    // A twitter message example in JSON format
    Text value = new Text("{\"filter_level\":\"low\",\"retweeted\":false,\"in_reply_to_screen_name\":null,\"truncated\":false,\"lang\":\"nl\",\"in_reply_to_status_id_str\":null,\"id\":662192658924851200,\"in_reply_to_user_id_str\":null,\"timestamp_ms\":\"1446714007489\",\"in_reply_to_status_id\":null,\"created_at\":\"Thu Nov 05 09:00:07 +0000 2015\",\"favorite_count\":0,\"place\":null,\"coordinates\":null,\"twinl_source\":[\"track\"],\"text\":\"Dit is een bericht van een zpper #KvK #ZZP\",\"contributors\":null,\"geo\":null,\"entities\":{\"symbols\":[],\"urls\":[],\"hashtags\":[],\"user_mentions\":[]},\"is_quote_status\":false,\"source\":\"<a href=\\http:\/\/ifttt.com\\ rel=\\nofollow\\>IFTTT<\/a>\",\"favorited\":false,\"in_reply_to_user_id\":null,\"retweet_count\":0,\"id_str\":\"662192658924851200\",\"user\":{\"location\":null,\"default_profile\":true,\"profile_background_tile\":false,\"statuses_count\":1616,\"lang\":\"nl\",\"profile_link_color\":\"0084B4\",\"id\":1637485430,\"following\":null,\"protected\":false,\"favourites_count\":0,\"profile_text_color\":\"333333\",\"verified\":false,\"description\":null,\"contributors_enabled\":false,\"profile_sidebar_border_color\":\"C0DEED\",\"name\":\"Sneek Weerbericht\",\"profile_background_color\":\"C0DEED\",\"created_at\":\"Thu Aug 01 08:30:59 +0000 2013\",\"default_profile_image\":false,\"followers_count\":6,\"profile_image_url_https\":\"https:\/\/pbs.twimg.com\/profile_images\/662161666440867840\/0g_hdeO8_normal.png\",\"geo_enabled\":false,\"profile_background_image_url\":\"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png\",\"profile_background_image_url_https\":\"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png\",\"follow_request_sent\":null,\"url\":null,\"utc_offset\":null,\"time_zone\":null,\"notifications\":null,\"profile_use_background_image\":true,\"friends_count\":0,\"profile_sidebar_fill_color\":\"DDEEF6\",\"screen_name\":\"SneekWeer\",\"id_str\":\"1637485430\",\"profile_image_url\":\"http:\/\/pbs.twimg.com\/profile_images\/662161666440867840\/0g_hdeO8_normal.png\",\"listed_count\":0,\"is_translator\":false},\"twinl_lang\":\"dutch\"}");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("1"), new Text("Hello world!"));   // Change this line
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("A simple tweet"));         // Change this line
    reduceDriver.withInput(new Text("2"), values);  // Change this line
    reduceDriver.withOutput(new Text("2"), new Text("A simple tweet"));  // Change this line
    reduceDriver.runTest();
  }


  @Test
  public void testMapReduce() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice tweet indeed\",\"id_str\":\"1\"}");  // Change this line
    mapReduceDriver.withInput(key, value);
    mapReduceDriver.withOutput(new Text("1"), new Text("A nice tweet indeed"));  // Change this line
    mapReduceDriver.runTest();
  }

}
