package com.janaa.flink_datastream.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.json.JSONObject;

import com.janaa.flink_datastream.utils.AgeCalculator;

public class MapAgeFunction implements MapFunction<String, String>{

	private AgeCalculator ageCalculator;
	public MapAgeFunction(AgeCalculator ageCalculator) {
		this.ageCalculator = ageCalculator;
	}
	
	@Override
	public String map(String value) throws Exception {
		JSONObject json = new JSONObject(value);
		String dob = json.getString("Date of Birth");
		int age = ageCalculator.getAge(dob);
		json.put("Age", age);
		return json.toString();
	}

}
