package nl.utwente.zzp;

import org.json.*;

public class Helper {

  public static final String KEY = "Polarity";
  
  public static void increaseCounter(JSONObject twitterMessage){
		/*
			This method can be replaced by JSONObject.increment function
			however now we can alter the data for e.g. normalise the polarity
		*/
    increaseCounter(twitterMessage,1);
  }

  public static void increaseCounter(JSONObject twitterMessage, int count){

		int curValue = 0;
		try{
    	curValue = twitterMessage.getInt(KEY);
		} catch (JSONException je){
			curValue = 0;
		} finally{
			curValue++;
		}

    // Modifying this method can be used to normalize the counter
		twitterMessage.put(KEY, curValue);
  }
  
  public static boolean hasPolarity(JSONObject twitterMessage){

		int curValue = 0;
		try{
    	curValue = twitterMessage.getInt(KEY);
		} catch (JSONException je){}

    return curValue > 0 ? true : false;
  }
}
