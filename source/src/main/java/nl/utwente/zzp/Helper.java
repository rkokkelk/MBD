package nl.utwente.zzp;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.json.*;

public class Helper {

  public static final String KEY = "Polarity";
  public static final SimpleDateFormat FORMATTER = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");
  public static final SimpleDateFormat OUTPUTTER = new SimpleDateFormat("dd/MM/yy");
  
  public static void increaseCounter(JSONObject twitterMessage){
		/*
			This method can be replaced by JSONObject.increment function
			however now we can alter the data for e.g. normalise the polarity
		*/
    increaseCounter(twitterMessage,1);
  }

  public static void increaseCounter(JSONObject twitterMessage, int count){

	int curValue = 0;
	try {
	  curValue = twitterMessage.getInt(KEY);
	} catch (JSONException je) {}
	
	curValue++;

	// Modifying this method can be used to normalize the counter
	twitterMessage.put(KEY, curValue);
  }
  
  public static boolean hasPolarity(JSONObject twitterMessage){

	int curValue = 0;
	try {
	  curValue = twitterMessage.getInt(KEY);
	} catch (JSONException je){}

    return curValue > 0;
  }

  public static boolean isZZP(int polarity){    
    // Use this method to alter data which is ZZP or not
    // based upon polarity
    return polarity >= 1;
  }

  public static Date parseDate(String input){
    try{
      return FORMATTER.parse(input);
    } catch (ParseException pe){
      System.err.println("Cannot parse input: " + input);
    }
    return null;
  } 

  public static String formatDate(Date input){
    return OUTPUTTER.format(input);
  } 

  public static Date getLatestDate(Date input1, Date input2){
    if(input1 == null) {
      return input2;
    } else if (input2 == null) {
      return input1;
    } else {
      if (input1.after(input2)) {
        return input1;
      } else {
        return input2;
      }
    }
  } 
}
