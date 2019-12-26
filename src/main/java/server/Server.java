package server;

import com.google.gson.Gson;
import kong.unirest.Unirest;
import spark.Request;
import spark.Response;
import util.*;

import java.sql.Time;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static spark.Spark.*;

/**
 * Entry point and controller of the server process
 * Opens up all API routes for the server
 * Holds all static metadata for use in the server
 */

public class Server {

    private Timestamp globalStableTime; // global stable time of the server
    private Timestamp localStableTime; // local stable time of the server
    private Timestamp[] versionVector; // version vector
    private Map<String, List<Item>> versionChain; // version chain
    private int replicaId; // replica id
    private int partitionId;
    private int numReplicas;
    private Logger logger;
    private ReentrantLock gstLock = new ReentrantLock(true);
    private ReentrantLock vvLock = new ReentrantLock(true);

    public Server(int partitionId, int replicaId, int numReplicas) {
        Timestamp now = new Timestamp(replicaId, partitionId);

        localStableTime = now;
        globalStableTime = now;

        versionVector = new Timestamp[numReplicas + 1];
        Arrays.fill(versionVector, now);

        versionChain = new HashMap<>();

        int portToBind = ServerConstants.BASE_PORT * replicaId + partitionId;
        this.replicaId = replicaId;
        this.partitionId = partitionId;
        this.numReplicas = numReplicas;

        port(portToBind);
        this.logger = new Logger(this);

        declareRoutes();

    }

    /**
     * Opens up all routes for use in the REST API
     */

    private void declareRoutes() {
        get(ServerConstants.SERVER_GET_PATH, this::processGetRequest);
        put(ServerConstants.SERVER_PUT_PATH, (req, res) -> this.processPutRequest(req));
        put(ServerConstants.SERVER_REPLICATE_PATH, (req, res) -> this.processReplicateRequest(req));
    }

    /**
     * Construct a failed GET response for the given item
     * @return - the payload corresponding to a failed GET response
     */

    private String constructFailedGetResponse() {
        return ResponseEnum.NOT_FOUND.toString();
    }

    /**
     * Construct a successful GET response for the given item
     * @param item - item to be inspected
     * @return - the payload corresponding to a successful GET response of the given item
     */

    private String constructSuccessfulGetResponse(Item item) {
        GetReply gr = new GetReply(item.getValue(), item.getUpdateTime(), this.globalStableTime);
        return new Gson().toJson(gr);
    }

    /**
     * Return the payload corresponding to the latest version of this key
     * @param key - key to query for
     * @return - the payload corresponding to the latest version of this key
     */

    private String getLatestVersion(String key) {

        if (!versionChain.containsKey(key)) {
            return constructFailedGetResponse();
        }

        List<Item> items = versionChain.get(key);
        items.sort(Comparator.comparing(Item::getUpdateTime));

        for (int i = items.size() - 1; i >= 0; i--) {
            Item currItem = items.get(i);

            if (currItem.getSourceReplica() == this.replicaId) {
                return constructSuccessfulGetResponse(currItem);
            }

            if (currItem.getUpdateTime().compareTo(this.globalStableTime) <= 0) {
                return constructSuccessfulGetResponse(currItem);
            }
        }

        return constructFailedGetResponse();
    }

    /**
     * Processes a GET request on the server side
     * @param req - the request to process
     * @return - the result of the GET request
     */

    private String processGetRequest(Request req, Response res) {
        String key = req.splat()[0];
        Timestamp time = new Timestamp(req.splat()[1]);

        this.logger.logPrint("Processing GET request with key: " + key + ", time: " + time.toString());
        this.logger.logPrint("Incoming Time: " + time);
        this.logger.logPrint("Current Time: " + this.globalStableTime.toString());

        /*
          Update global stable time appropriately
         */

        if (globalStableTime.compareTo(time) < 0) {
            globalStableTime = time;
        }

        System.out.println("GST: " + globalStableTime.toString());
        return this.getLatestVersion(key);
    }

    /**
     * Update the version chain with a new item
     * @param d - the item to update with
     */

    private void addVersion(Item d) {
        String key = d.getKey();
        List<Item> currChain = this.versionChain.getOrDefault(key, new ArrayList<>());
        currChain.add(d);
        this.versionChain.put(key, currChain);
    }

    /**
     * Processes a GET request on the server side
     * @param req - the request to process
     * @return - the result of the GET request
     */

    private String processPutRequest(Request req) {
        System.out.println(Arrays.toString(req.splat()));
        String key = req.splat()[0];
        String value = req.splat()[1];
        Timestamp ts = new Timestamp(req.splat()[2]);

        long clockTime = waitUntil(ts);

        versionVector[this.replicaId] = new Timestamp()
                .timestamp(clockTime)
                .replicaId(this.replicaId)
                .partitionId(this.partitionId);

        Item d = new Item(key, value, versionVector[this.replicaId], this.replicaId);
        addVersion(d);
        broadcastReplicateRequest(d);
        return constructSuccessfulPutResponse(versionVector[this.replicaId]);
    }

    /**
     * Broadcast a replicate request to all other same partitions in different datacenters
     * @param d - the item to replicate
     */

    private void broadcastReplicateRequest(Item d) {

        for (int i = 1; i < this.numReplicas + 1; i++) {

            if (i == this.replicaId) {
                continue;
            }

            int port = ServerConstants.BASE_PORT * i + this.partitionId;

            try {
                Unirest.put("http://localhost:{port}/replicate/{id}/{item}")
                        .routeParam("port", Integer.toString(port))
                        .routeParam("id", Integer.toString(this.replicaId))
                        .routeParam("item", d.toString())
                        .asString();

            } catch (kong.unirest.UnirestException ke) {
//                ke.printStackTrace();
            }
        }
    }

    /**
     * Constructs a successful PUT response
     * @param ts - timestamp to be used in response
     * @return - payload for successful PUT response
     */

    private String constructSuccessfulPutResponse(Timestamp ts) {
        PutReply pr = new PutReply(ts);
        return new Gson().toJson(pr);
    }

    /**
     * Waits until the given clock time of the timestamp dt. Returns the time that was last elapsed during the waiting
     * period
     * @param dt - the timestamp to wait for
     * @return - the time last elapsed that satisfies dt
     */

    private long waitUntil(Timestamp dt) {
        long timeToWaitFor = dt.getClockTime();
        long currTime = System.nanoTime();

        while (currTime <= timeToWaitFor) {

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
//                e.printStackTrace();
            }

            currTime = System.nanoTime();
        }

        return currTime;
    }

    /**
     * Process a replicate request from the same partition in another data center
     * @param req - the replicate request
     * @return - a received response - same every time
     */

    private String processReplicateRequest(Request req) {
        this.logger.logPrint("Processing Replicate Request");
        int replicaReceivedFrom = Integer.parseInt(req.splat()[0]);
        Item d = new Item(req.splat()[1]);

        addVersion(d);
        this.versionVector[replicaReceivedFrom] = d.getUpdateTime();

        return ResponseEnum.RECEIVED.toString();
    }

    public int getReplicaId() {
        return this.replicaId;
    }

    public int getPartitionId() {
        return this.partitionId;
    }

    public void setGlobalStableTime(Timestamp globalStableTime) {
        try {
            gstLock.lock();
            this.globalStableTime = globalStableTime;

        } finally {
            gstLock.unlock();
        }
    }

    public Timestamp[] getVersionVector() {
        Timestamp[] vv;

        try {
            vvLock.lock();
            vv = this.versionVector;

        } finally {
            vvLock.unlock();
        }

        return vv;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public static void main(String[] args) {
        int partitionId = Integer.parseInt(args[0]); // the partition id that this server represents
        int replicaId = Integer.parseInt(args[1]); // the replica id that this server is part of and responds to
        int numReplicas = Integer.parseInt(args[2]); // number of total replicas or data centers

//        int partitionId = 4; // the partition id that this server represents
//        int replicaId = 2; // the replica id that this server is part of and responds to
//        int numReplicas = 5; // number of total replicas or data centers
        new Server(partitionId, replicaId, numReplicas);
    }
}
