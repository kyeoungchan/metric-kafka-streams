package com.example.metrickafkastreams;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MetricJsonUtils {

    public static double getTotalCpuPercent(String value) {
        return ((JsonObject) new JsonParser().parseString(value)).get("system")
                .getAsJsonObject().get("total")
                .getAsJsonObject().get("norm")
                .getAsJsonObject().get("pct").getAsDouble();
    }

    public static String getMetricName(String value) {
        return ((JsonObject) new JsonParser().parseString(value)).get("metricset")
                .getAsJsonObject().get("name")
                .getAsString();
    }

    public static String getHostTimestamp(String value) {
        JsonObject objectValue = (JsonObject) new JsonParser().parseString(value);
        JsonObject result = objectValue.getAsJsonObject("host");
        result.add("timestamp", objectValue.get("@timestamp"));
        return result.toString();
    }
}
