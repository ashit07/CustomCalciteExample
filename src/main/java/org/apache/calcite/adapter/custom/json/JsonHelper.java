package org.apache.calcite.adapter.custom.json;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;

public class JsonHelper {
	public static Iterator<Object> getJsonArr(String json, String arrayParam) {
		JSONArray jsonArr = new JSONObject(json).getJSONArray(arrayParam);
		return jsonArr.iterator();
	}
}
