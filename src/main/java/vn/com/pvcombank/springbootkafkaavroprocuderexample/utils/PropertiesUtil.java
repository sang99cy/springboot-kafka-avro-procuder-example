package vn.com.pvcombank.springbootkafkaavroprocuderexample.utils;

import java.util.Properties;

public class PropertiesUtil {
    public static Properties properties(String[] args) {
        Properties properties = new Properties();
        properties.putAll(System.getProperties());
        for (String arg : args) {
            if (arg.contains("=")) {
                String[] split = arg.split("=");
                properties.put(split[0], split[1]);
            }
        }
        return properties;
    }

    public static String property(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = System.getenv(key.toUpperCase().replace(".", "_"));
            if (value == null) {
                value = defaultValue;
            }
        }
        return value;
    }
}