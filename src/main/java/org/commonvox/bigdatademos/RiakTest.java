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

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.buckets.FetchBucketProperties;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.core.query.Namespace;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.spark_project.jetty.io.ArrayByteBufferPool.Bucket;

/**
 *
 * @author Daniel Vimont
 */
public class RiakTest {
    public static void main(String [] args) throws UnknownHostException, ExecutionException, InterruptedException {

//        // Riak Client with supplied IP and Port
//        RiakClient client = RiakClient.newClient(8087, "172.16.1.34");
//
//        client.shutdown();
        
        // Riak Client with multiple node connections
        LinkedList<String> ipAddresses = new LinkedList<String>();
        ipAddresses.add("ec2-34-233-230-40.compute-1.amazonaws.com");
        ipAddresses.add("ec2-34-230-235-214.compute-1.amazonaws.com");
        ipAddresses.add("ec2-34-197-52-251.compute-1.amazonaws.com");
        System.out.println("About to instantiate RiakClient! Could throw UnknownHostException!!");
        RiakClient myNodeClient = RiakClient.newClient(8087, ipAddresses);
        
        List<RiakNode> riakNodes = myNodeClient.getRiakCluster().getNodes();
        for (RiakNode node : riakNodes) {
            System.out.println("Node address: " + node.getRemoteAddress());
            System.out.println("Node port: " + node.getPort());
            System.out.println("Node state: " + node.getNodeState().toString());
        }
        
        System.out.println("Creating bucket");
        createBucket(myNodeClient, "TestBucket");
        
        String myData = "This is my data";
//        Bucket myBucket = myNodeClient..fetchBucket("TestBucket").execute();
//        myBucket.store("TestKey", myData).execute();
        
        
        System.out.println("Shutting down client!");
        // NOTE: #shutdown may hang indefinitely:
        //   https://github.com/basho/riak-java-client/issues/706
//        myNodeClient.shutdown();
        for (RiakNode node : riakNodes) {
            System.out.println("Shutting down NODE: " + node.getRemoteAddress());
            node.shutdown();
        }
      
        
    }
    
    static void createBucket (RiakClient riakClient, String bucketName) throws ExecutionException, InterruptedException {
       Namespace ns = new Namespace(bucketName);

        // If the bucket does not exist in Riak, it will be created with the default properties when you query for them. 
        FetchBucketProperties fetchProps = new FetchBucketProperties.Builder(ns).build();

        FetchBucketPropsOperation.Response fetchResponse = riakClient.execute(fetchProps);
        BucketProperties bp = fetchResponse.getBucketProperties();

        // By using the StoreBucketProperties command, 
        // you can specify properties' values. 
        //
        // If the bucket already exists in Riak, the bucket 
        // properties will be updated.
        //
        // Only those properties that you specify will be updated, 
        // there is no need to fetch the bucket properties to edit them.
        StoreBucketProperties storeProps = 
            new StoreBucketProperties.Builder(ns)
            .withNVal(2).withR(1).build();

        riakClient.execute(storeProps);
        
    }
}
