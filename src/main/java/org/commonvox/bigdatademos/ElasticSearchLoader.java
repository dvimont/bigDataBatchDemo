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
package org.commonvox.bigdatademos;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Daniel Vimont
 */
public class ElasticSearchLoader {

    private static String esIndex; 
    private static String esType; 
    private static JSONParser jsonParser = new JSONParser();
    private static Long lineCounter = 0L;
    
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
          System.err.println("Usage: ElasticSearchLoader <elasticSearch-index> "
                  + "<elasticSearch-type> <input-directory>");
          System.exit(-1);
        }
        esIndex = args[0];               // e.g. "pageviews";
        esType = args[1];                // e.g. "interval";
        String inputDirectory = args[2]; // e.g. "./sampleJsonForES/"
        loadJsonToES(inputDirectory);
    }
    
    private static void loadJsonToES (String inputDirectory) throws IOException {
        try (RestClient lowLevelClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")).build() ) {
            
            RestHighLevelClient client = new RestHighLevelClient(lowLevelClient);
        
            for (String absoluteFilePath : getFileSet(inputDirectory)) {
                System.out.println("** Bulk processing commencing for file: " + absoluteFilePath);
                try (Stream<String> stream = Files.lines(Paths.get(absoluteFilePath))) {
                    BulkRequest bulkRequest = new BulkRequest();
                    lineCounter = 0L;
                    stream.forEach(line -> loadJsonObject(bulkRequest, line, absoluteFilePath));
                    if (bulkRequest.requests().size() > 0) {
                        BulkResponse bulkResponse = client.bulk(bulkRequest);
                        assessResponses(bulkResponse);
                    }
                }
            }
        }
    }
    
    private static void loadJsonObject(BulkRequest bulkRequest, String line, String fileName) {
        ++lineCounter;
        if (line.isEmpty()) {
            return;
        }
        int firstCommaPosition = line.indexOf(','); // 9--daily, 7--monthly, 5--yearly
        String docId = line.substring(1, firstCommaPosition);
        String jsonObject = line.substring(firstCommaPosition + 1, line.length() - 1);
        if (isValidJSON(jsonObject)) {
            // System.out.println("**** Adding JSON with the following docId to bulkRequest: " + docId);
            bulkRequest.add(new IndexRequest(esIndex, esType, docId)  
                    .source(jsonObject, XContentType.JSON));
        } else {
            System.out.println("**** Invalid JSON encountered in input file: "
                    + fileName + "line: " + lineCounter);
        }
    }
     
    private static void assessResponses(BulkResponse responses) {
        System.out.println("** Bulk responses start...");
        for (BulkItemResponse bulkItemResponse : responses) { 
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            String responseStatus;
            String responseResult;
            if (itemResponse == null) {
                responseStatus = "null";
                responseResult = "null";
            } else {
                responseStatus = itemResponse.status().name();
                responseResult = itemResponse.getResult().toString();
            }
            System.out.println("**** Response status: <" + responseStatus
                    + ">. Details: <" + responseResult + ">");
        }
        System.out.println("** Bulk responses end");
    }
    
    private static Set<String> getFileSet(String directory) {
        Set<String> fileNames = new TreeSet<>();
        try (DirectoryStream<java.nio.file.Path> directoryStream = Files.newDirectoryStream(Paths.get(directory))) {
            for (java.nio.file.Path path : directoryStream) {
                if (Files.isDirectory(path)) {
                    fileNames.addAll(getFileSet(path.toString()));
                } else {
                    fileNames.add(path.toFile().getAbsolutePath());
                }
            }
        } catch (IOException ex) {}
        return fileNames;
    }
    
    
    public static boolean isValidJSON(String test) {
        try {
            jsonParser.parse(test);
        } catch (ParseException ex) {
            return false;
        }
        return true;
    }    
}
