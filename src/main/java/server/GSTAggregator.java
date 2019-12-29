package server;

import kong.unirest.Unirest;
import util.MessageBox;
import util.Timestamp;
import java.util.LinkedList;
import java.util.Queue;

/**
 * This service will aggregate the minimum LST among its children nodes
 * It will push the result to the parent node
 * When the parent node pushes the result back down, this thread will then update the GST of the server
 */

public class GSTAggregator extends MessageBox {

    private Integer leftChild; // partitionId of left child
    private Integer rightChild; // partitionId of right child
    private Integer leftPort; // port of left child
    private Integer rightPort; // port of right child
    private Integer parentPort; // parent port
    private int startupTime;
    private Queue<Timestamp> leftQueue; // FIFO queue for messages from the left child
    private Queue<Timestamp> rightQueue; // FIFO queue for messages from the right child
    private boolean isLeaf;

    /**
     * Returns whether this server id exists in the current setup as a child
     * @param id - the server id to check
     * @return - whether this id is valid
     */

    private static boolean isValidChild(int id) {
        return id <= ServerContext.getServer().getNumPartitions();
    }

    /**public
     * Returns whether this server id exists in the current setup as a parent
     * @param id - the server id to check
     * @return - whether this id is valid
     */

    private static boolean isValidParent(int id) {
        return id > 0;
    }

     GSTAggregator() {
        this.startupTime = 10000;
        int currId = ServerContext.getServer().getPartitionId();
        int leftId = 2 * currId;
        int rightId = 2 * currId + 1;
        int parentId = currId / 2;
        debug("left child: " + leftId);
        debug("num replicas: " + ServerContext.getServer().getNumPartitions());

        if (isValidChild(leftId)) {
            this.leftChild = leftId;
            this.leftPort = ServerConstants.BASE_PORT * ServerContext.getServer().getReplicaId() + leftId;
            this.leftQueue = new LinkedList<>();
        }

        if (isValidChild(rightId)) {
            this.rightChild = rightId;
            this.rightPort = ServerConstants.BASE_PORT * ServerContext.getServer().getReplicaId() + rightId;
            this.rightQueue = new LinkedList<>();
        }

        if (this.leftChild == null && this.rightChild == null) {
            this.isLeaf = true;
        }

        if (isValidParent(parentId)){
            // partitionId of parent
            this.parentPort = ServerConstants.BASE_PORT * ServerContext.getServer().getReplicaId() + parentId;
        }
    }

    /**
     * Create payload for pushing up the tree
     * @param ts - timestamp to push
     * @return - payload for pushing up the tree
     */

    public String createPayloadForPushUp(Timestamp ts) {
        String[] tokens = new String[3];

        tokens[0] = AggregationEnum.LOCAL_MIN_LST.toString();
        tokens[1] = Integer.toString(ServerContext.getServer().getPartitionId());
        tokens[2] = ts.toString();

        return String.join(" ", tokens);
    }

    /**
     * Create payload for pushing down the tree
     * @param ts - timestamp to push
     * @return - payload for pushing down the tree
     */

    private String createPayloadForPushDown(Timestamp ts){
        String[] tokens = new String[3];

        tokens[0] = AggregationEnum.GLOBAL_MIN_LST.toString();
        tokens[1] = Integer.toString(ServerContext.getServer().getPartitionId());
        tokens[2] = ts.toString();

        return String.join(" ", tokens);
    }

    /**
     * Returns the min timestamp in the version vector
     * @return - the minimum VV timestamp
     */

    public Timestamp getMinVVTimestamp() {
        Timestamp[] currVV = ServerContext.getServer().getVersionVector();
        Timestamp currMin = currVV[0];

        for (int i = 1; i < currVV.length; i++) {
            if (currMin == null) {
                if (currVV[i] != null) {
                    currMin = currVV[i];
                }
                continue;
            }

            if (currVV[i] == null) {
                continue;
            }

            if (currMin.compareTo(currVV[i]) > 0) {
                currMin = currVV[i];
            }
        }

        return currMin;
    }
    /**
     * Pushes an aggregation up the tree
     */

    private void pushUp() {
        assert(this.parentPort != null);
        Timestamp myMin = this.getMinVVTimestamp();

        if (leftQueue != null && rightQueue != null) {

            if (leftQueue.size() > 0 && rightQueue.size() > 0) {

                Timestamp leftLST = leftQueue.poll();
                Timestamp rightLST = rightQueue.poll();
                assert rightLST != null;
                Timestamp minLST = (leftLST.compareTo(rightLST) <= 0) ? leftLST : rightLST;
                Timestamp trueMinLST = (minLST.compareTo(myMin) <= 0) ? minLST : myMin;

                Unirest.put("http://localhost:{port}/aggregate/{payload}")
                        .routeParam("port", this.parentPort.toString())
                        .routeParam("payload", createPayloadForPushUp(trueMinLST))
                        .asString();

                return;
            }
        }

        if (leftQueue != null) {

            Timestamp leftLST = leftQueue.poll();
            assert leftLST != null;
            Timestamp trueMinLST = (leftLST.compareTo(myMin) <= 0) ? leftLST : myMin;

            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.parentPort.toString())
                    .routeParam("payload", createPayloadForPushUp(trueMinLST))
                    .asString();

            return;
        }

        if (rightQueue != null) {

            Timestamp rightLST = rightQueue.poll();
            assert rightLST != null;
            Timestamp trueMinLST = (rightLST.compareTo(myMin) <= 0) ? rightLST : myMin;

            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.parentPort.toString())
                    .routeParam("payload", createPayloadForPushUp(trueMinLST))
                    .asString();

        }
    }

    /**
     * Pushes an aggregation down the tree
     */

    private void pushDown() {
        assert(this.parentPort == null);
        Timestamp myMin = this.getMinVVTimestamp();

        if (leftQueue != null && rightQueue != null) {

            if (leftQueue.size() > 0 && rightQueue.size() > 0) {

                Timestamp leftLST = leftQueue.poll();
                Timestamp rightLST = rightQueue.poll();
                assert rightLST != null;
                Timestamp minLST = (leftLST.compareTo(rightLST) <= 0) ? leftLST : rightLST;
                Timestamp trueMinLST = (minLST.compareTo(myMin) <= 0) ? minLST : myMin;

                Unirest.put("http://localhost:{port}/aggregate/{payload}")
                        .routeParam("port", Integer.toString(leftPort))
                        .routeParam("payload", createPayloadForPushDown(trueMinLST))
                        .asString();

                Unirest.put("http://localhost:{port}/aggregate/{payload}")
                        .routeParam("port", Integer.toString(rightPort))
                        .routeParam("payload", createPayloadForPushDown(trueMinLST))
                        .asString();


                return;
            }
        }

        if (leftQueue != null) {

            Timestamp leftLST = leftQueue.poll();
            assert leftLST != null;
            Timestamp trueMinLST = (leftLST.compareTo(myMin) <= 0) ? leftLST : myMin;

            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.leftPort.toString())
                    .routeParam("payload", createPayloadForPushDown(trueMinLST))
                    .asString();

            return;
        }

        if (rightQueue != null) {

            Timestamp rightLST = rightQueue.poll();
            assert rightLST != null;
            Timestamp trueMinLST = (rightLST.compareTo(myMin) <= 0) ? rightLST : myMin;

            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.rightPort.toString())
                    .routeParam("payload", createPayloadForPushDown(trueMinLST))
                    .asString();

        }
    }

    /**
     * Pushes a message to the parent node
     * @param tokens - the message to be pushed
     */

    private void pushMessages(String[] tokens) {
        assert (AggregationEnum.valueOf(tokens[0]) == AggregationEnum.LOCAL_MIN_LST); // sanity check

        int id = Integer.parseInt(tokens[1]);
        Timestamp lst = new Timestamp(tokens[2]);

        debug("Is Leaf: " + this.isLeaf);

        if (this.isLeaf) {
            debug("" + this.parentPort);
            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.parentPort.toString())
                    .routeParam("payload", createPayloadForPushUp(lst))
                    .asString();

            return;
        }

        if (leftChild != null && id == leftChild) {
            leftQueue.add(lst);
        }

        else if (rightChild != null && id == rightChild) {
            rightQueue.add(lst);
        }

        if (this.parentPort == null) { // we are at the root, we now must push the messages down
            pushDown();
        }

        else {
            pushUp();
        }
    }

    /**
     * Pushes a payload down the tree to the left and right children
     * @param payload - the payload to push down
     */

    private void forwardDown(String payload) {
        if (this.leftPort != null) {

            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.leftPort.toString())
                    .routeParam("payload", payload)
                    .asString();
        }

        if (this.rightPort != null) {

            Unirest.put("http://localhost:{port}/aggregate/{payload}")
                    .routeParam("port", this.rightPort.toString())
                    .routeParam("payload", payload)
                    .asString();
        }
    }

    /**
     * Processes a message that is in the message queue of this worker
     * @param msg - the message to be processed
     */

    private void processMessage(String msg) {

        String[] tokens = msg.split(" ");
        AggregationEnum messageType = AggregationEnum.valueOf(tokens[0]);

        switch (messageType) {

            case LOCAL_MIN_LST: // we need to push this message up
                pushMessages(tokens);
                break;

            case GLOBAL_MIN_LST: // this is the time that is pushed down - we set this server's GST to this value
                Timestamp currTS = new Timestamp(tokens[2]);
                ServerContext.getServer().setGlobalStableTime(currTS);
                debug("Changed GST to " + currTS);
                forwardDown(msg);
                break;
        }
    }

    /**
     * Sleeps 10 seconds before truly starting
     */

    private void waitForStart() {
        try {
            Thread.sleep(this.startupTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    private void debug(String s) {
        ServerContext.getServer().getLogger().logPrint(s);
    }

    /**
     * Execute the aggregation service
     */

    public void run() {
        waitForStart();
        debug("GST Aggregator is now beginning");

        while (true) {
            String msg = this.pollMessage();
            debug("Polling message of: " + msg);
            processMessage(msg);
        }
    }

}
