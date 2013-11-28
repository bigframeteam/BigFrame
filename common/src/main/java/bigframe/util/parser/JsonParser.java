package bigframe.util.parser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonParser {

	public JsonParser() {
		// TODO Auto-generated constructor stub
	}

	public static JSONObject parseJsonFromFile(InputStream file) {
		JSONParser parser = new JSONParser();
		try {

			Object obj = parser.parse(new BufferedReader(new InputStreamReader(file)));

			JSONObject jsonObject = (JSONObject) obj;

			return jsonObject;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
	
	public static JSONObject parseJsonFromString(String json_str) {
		JSONParser parser = new JSONParser();
		try {

			Object obj = parser.parse(json_str);

			JSONObject jsonObject = (JSONObject) obj;

			return jsonObject;

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
}
