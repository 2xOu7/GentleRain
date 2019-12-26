package client;

import spark.Request;
import spark.Response;
import util.ClientServerEnum;
import util.GetReply;
import util.ResponseEnums;
import util.Timestamp;
import com.google.gson.*;
import static spark.Spark.*;

/**
 * Entry point and controller of the client process
 * Opens up all API routes for the client
 * Holds all static metadata for use in the client
 */

public class Client extends Thread {

    private ClientHandler clientHandler;
    private Timestamp dependencyTime;
    private Timestamp globalStableTime;

    public Client(int numPartitions, int replicaId) {
        clientHandler = new ClientHandler(replicaId, numPartitions);

        Timestamp now = new Timestamp(replicaId, -1);
        dependencyTime = now;
        globalStableTime = now;

        int portToBind = ClientConstants.CLIENT_BASE_PORT + replicaId;
        port(portToBind); // bind the client to the given port

        /*******************************************************************
         * GET route for a given key
         *******************************************************************/

        get(ClientConstants.CLIENT_GET_PATH, this::processGetRequest);

        /*******************************************************************
         * PUT route for a given key value pair
         *******************************************************************/

        put(ClientConstants.CLIENT_PUT_PATH, (req, res) -> this.processPutRequest(req));
    }

    /**
     * Processes a get request
     * @param req - an HTTP request in the Spark format
     * @return - return the result of the get request
     */

    private String processGetRequest(Request req, Response res) {
        res.header("Content-Type", "application/json");
        String key = req.params(ClientConstants.KEY_PARAM); // get the key
        String clientHandlerMsg = createGetReqMessage(key); // create a get request message involving the key

        String response = fetchResultFromClientHandler(clientHandlerMsg); // return the result of the GET request
        GetReply reply = new Gson().fromJson(response, GetReply.class);
        return response;
    }

    /**
     * Creates a PUT request message for the client handler to process
     * @param key - key
     * @param value - value
     * @param dependencyTime - dependency time
     * @return - PUT REQ payload
     */

    private String createPutReqMessage(String key, String value, Timestamp dependencyTime) {
        String[] tokens = new String[5];

        tokens[0] = ClientServerEnum.PUT_REQ.toString();
        tokens[1] = key;
        tokens[2] = value;
        tokens[3] = dependencyTime.toString();

        return String.join(" ", tokens);
    }

    /**
     * Returns the body of the put request
     * @param req - the request
     * @return = a message indicating whether or not the message has been received
     */

    private String processPutRequest(Request req) {
        String key = req.splat()[0];
        String value = req.splat()[1];

        Timestamp dependencyTime = this.dependencyTime;
        String clientHandlerMsg = createPutReqMessage(key, value, dependencyTime);
        return fetchResultFromClientHandler(clientHandlerMsg);
    }

    /**
     * Dispatches a message for processing to the client handler and returns the value retrieved
     * @param msg - the msg to be dispatched to the client handler
     */

    private String fetchResultFromClientHandler(String msg) {
        return clientHandler.processMessage(msg);
    }

    /**
     * Creates a get request message for the client handler to process
     * @param key - the key of the get request
     * @return - the get request message payload
     */

    private String createGetReqMessage(String key) {
        String[] tokens = new String[3];

        tokens[0] = ClientServerEnum.GET_REQ.toString();
        tokens[1] = key;
        tokens[2] = globalStableTime.toString();

        return String.join(" ", tokens);
    }

    public static void main(String[] args) {
        int numPartitions = Integer.parseInt(args[0]); // number of partitions in the data center
        int replicaId = Integer.parseInt(args[1]); // replica id or the id of this data center

        new Client(numPartitions, replicaId);
    }
}
