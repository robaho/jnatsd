package com.robaho.jnatsd.util;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

import java.lang.reflect.Field;

public class JSON {
    /**
     * create JSON representation of Object.
     * @param o the non-null object
     * @return the JSON string
     */
    public static String save(Object o){
        JsonObject json = new JsonObject();
        for(Field field : o.getClass().getDeclaredFields()){
            try {
                if (field.getType() == int.class) {
                    json.add(field.getName(), (int) field.get(o));
                } else {
                    json.add(field.getName(), (String)field.get(o));

                }
            } catch (IllegalAccessException ignored) {
            }
        }
        return json.toString();
    }

    /**
     * initialize a Object (using reflection) from a JSON object.
     * <b>Note: only top-level properties are supported at this time</b>
     * @param json the JSON object as a String
     * @param o the non-null Object to be initialized
     */
    public static void load(String json, Object o){
        JsonObject jo = Json.parse(json).asObject();
        for(JsonObject.Member m : jo){
            try {
                Field field = o.getClass().getField(m.getName());
                if(field.getType()==int.class){
                    field.setInt(o,m.getValue().asInt());
                } else {
                    field.set(o,m.getValue().asString());
                }
            } catch (Exception ignored) {
            }
        }
    }
}
