
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakNode;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

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
 *
 * @author Daniel Vimont
 */
public class RiakTest {
    public static void main(String [] args) throws UnknownHostException {

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
        }
        System.out.println("Shutting down client!");
        myNodeClient.shutdown();
        
    }
}
