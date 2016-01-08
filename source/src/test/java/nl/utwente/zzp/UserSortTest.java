import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import nl.utwente.bigdata.UserSort;
 
public class UserSortTest {
 
  private MapDriver<Object, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver; 
 
  @Before
  public void setUp() {
    UserSort.UserMapper mapper = new UserSort.UserMapper();
    UserSort.UserReducer reducer = new UserSort.UserReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice tweet indeed\",\"id_str\":\"2\",\"user\":{\"name\":\"foobar\", \"id\":\"123456\"}}");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("2"), new Text("A nice tweet indeed"));
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("foobar"));
    reduceDriver.withInput(new Text("2"), values);
    reduceDriver.withOutput(new Text("2"), new Text("foobar"));
    reduceDriver.runTest();
  }


  @Test
  public void testMapReduce() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"A nice tweet indeed\",\"id_str\":\"2\",\"user\":{\"name\":\"foobar\", \"id\":\"123456\"}}");
    mapReduceDriver.withInput(key, value);
    mapReduceDriver.withOutput(new Text("2"), new Text("A nice tweet indeed"));
    mapReduceDriver.runTest();
  }

}
