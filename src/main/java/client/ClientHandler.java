package client;

import util.ClientHandlerEnum;
import kong.unirest.*;

/***
 * Handler that is used by the client to communicate over REST API
 * Marshals string into JSON to send over REST API
 * Executes all REST API calls
 */

public class ClientHandler {

    private int numPartitions;
    private int replicaId;

    public ClientHandler(int replicaId, int numPartitions) {
        this.replicaId = replicaId;
        this.numPartitions = numPartitions;
    }

    /**
     * Returns the port that is responsible for the key via consistent hashing
     * @param key - the key used to calculate the consistent hash for
     * @return - the port to forward to
     */

    private int calculatePortToForward(String key) {
        int partitionToForward = (Math.abs(key.hashCode()) % numPartitions) + 1;
        System.out.println("Forwarding to replica " + replicaId + " and partition " + partitionToForward);
        return ClientConstants.SERVER_BASE_PORT * replicaId + partitionToForward;
    }

    /**
     * Sends a GET request to the given port and returns back the response
     * @param key - the key in the get request
     * @param time - the time in the get request
     * @param port - the port in the get request
     * @return - the response of the request
     */

    private String sendGetReq(String key, String time, int port) {
        HttpResponse<String> response = Unirest.get("http://localhost:{port}/get/{key}/{time}")
                .routeParam("port", Integer.toString(port))
                .routeParam("key", key)
                .routeParam("time", time)
                .asString();

        return response.getBody();
    }

    /**
     * Sends a PUT request to a server with key, value, and timestamp
     * @param key - key to send
     * @param value - value to send
     * @param timestamp - dependency time of the client
     * @param port - port to forward to
     * @return - response of the request
     */

    private String sendPutReq(String key, String value, String timestamp ,int port) {
        HttpResponse<String> response = Unirest.put("http://localhost:{port}/put/{key}/{value}/{timestamp}")
                .routeParam("port", Integer.toString(port))
                .routeParam("key", key)
                .routeParam("value", value)
                .routeParam("timestamp", timestamp)
                .asString();

        System.out.println("Sending PUT req with " + key + " " + value + " " + timestamp);
        return response.getBody();
    }

    /**
     * Processes a GET request
     * @param tokens -the tokens of the request
     * @return - the result of the GET request
     */

    private String resolveGetReq(String[] tokens) {
        ClientHandlerEnum command = ClientHandlerEnum.valueOf(tokens[0]);
        assert(command == ClientHandlerEnum.GET_REQ);

        String key = tokens[1];
        String time = tokens[2];
        int portToForward = calculatePortToForward(key);

        return this.sendGetReq(key, time, portToForward);
    }

    /**
     * Processes a PUT request
     * @param tokens - the tokens of the request
     * @return - the result of the PUT request
     */

    private String resolvePutReq(String[] tokens) {
        ClientHandlerEnum command = ClientHandlerEnum.valueOf(tokens[0]);
        assert(command == ClientHandlerEnum.PUT_REQ);

        String key = tokens[1];
        String value = tokens[2];
        String dependencyTime = tokens[3];
        int portToForward = calculatePortToForward(key);

        return this.sendPutReq(key, value, dependencyTime, portToForward);
    }

    /**
     * Client Handler will process the message by forwarding it to the correct partition
     * @param msg - msg to be forwarded to a server
     * @return - the result of the request
     */

    public String processMessage(String msg) {
        String messageDelimiter = " ";
        String[] tokens = msg.split(messageDelimiter);
        ClientHandlerEnum command = ClientHandlerEnum.valueOf(tokens[0]);

        switch (command) {

            /*******************************************************************
             * Resolve GET Request
             *******************************************************************/

            case GET_REQ:
                return resolveGetReq(tokens);

            /*******************************************************************
             * Resolve PUT Request
             *******************************************************************/

            case PUT_REQ:
                return resolvePutReq(tokens);
        }

        return "";
    }
}
