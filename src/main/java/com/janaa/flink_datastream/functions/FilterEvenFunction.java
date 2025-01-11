package com.janaa.flink_datastream.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.json.JSONObject;

public class FilterEvenFunction implements FilterFunction<String>{

	@Override
	public boolean filter(String value) throws Exception {
		boolean result = false;
		JSONObject json = new JSONObject(value);
		int age = json.getInt("Age");
		if(age % 2 == 0)
			result = true;
		return result;
	}

}
