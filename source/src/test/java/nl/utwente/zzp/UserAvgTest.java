import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import nl.utwente.bigdata.UserAvg;
 
public class UserAvgTest {
 
  private MapDriver<Object, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver; 
 
  @Before
  public void setUp() {
    UserAvg.UserMapper mapper = new UserAvg.UserMapper();
    UserAvg.UserReducer reducer = new UserAvg.UserReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice tweet indeed\",\"id_str\":\"2\",\"user\":{\"name\":\"foobar\", \"id\":\"123456\"}}");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("123456"), new IntWritable(19));
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(11));
    values.add(new IntWritable(13));
    reduceDriver.withInput(new Text("2"), values);
    reduceDriver.withOutput(new Text("2"), new IntWritable(12));
    reduceDriver.runTest();
  }


  @Test
  public void testMapReduce() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice tweet \",\"id_str\":\"2\",\"user\":{\"name\":\"foobar\", \"id\":\"123456\"}}");
    Text value2 = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice twee\",\"id_str\":\"2\",\"user\":{\"name\":\"foobar\", \"id\":\"123456\"}}");
    mapReduceDriver.withInput(key, value);
    mapReduceDriver.withInput(key, value2);
    mapReduceDriver.withOutput(new Text("123456"), new IntWritable(12));
    mapReduceDriver.runTest();
  }

}