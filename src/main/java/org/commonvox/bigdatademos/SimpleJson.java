package org.commonvox.bigdatademos;

/*
 * Copyright 2017 Daniel Vimont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Simple tools for constructing JSON output.
 * 
 * @author Daniel Vimont
 */
public class SimpleJson {
    
    public static final String OBJECT_OPEN = "{";
    public static final String OBJECT_CLOSE = "}";
    public static final String ARRAY_OPEN = "[";
    public static final String ARRAY_CLOSE = "]";
    public static final String DELIMITER = ",";
    
    public static String object(String text) {
        StringBuilder stringBuilder = new StringBuilder();
        return stringBuilder.append("{").append(text).append("}").toString();
    }
    
    public static String nameValuePair(String name, String value) {
        StringBuilder stringBuilder = new StringBuilder();
        return stringBuilder.append("\"").append(name).append("\":\"")
                .append(value).append("\"").toString();
    }
    
//    public static String array(Iterable<String> entries) {
//        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append("[");
//        boolean pastFirst = false;
//        for (String entry : entries) {
//            if (!pastFirst) {
//                pastFirst = true;
//            } else {
//                stringBuilder.append(",");
//            }
//            stringBuilder.append(entry);
//        }
//        stringBuilder.append("]");
//        return stringBuilder.toString();
//    }
    
}
