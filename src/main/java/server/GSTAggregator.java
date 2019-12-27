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

    private Integer parent; // partitionId of parent
    private Integer leftChild; // partitionId of left child
    private Integer rightChild; // partitionId of right child
    private Integer leftPort; // port of left child
    private Integer rightPort; // port of right child
    private Integer parentPort; // parent port

    private Queue<Timestamp> leftQueue; // FIFO queue for messages from the left child
    private Queue<Timestamp> rightQueue; // FIFO queue for messages from the right child

    private static boolean isValidChild(int id) {
        return id <= ServerContext.getServer().getNumReplicas();
    }

    private static boolean isValidParent(int id) {
        return id > 0;
    }

    public GSTAggregator() {
        int currId = ServerContext.getServer().getPartitionId();
        int leftId = 2 * currId;
        int rightId = 2 * currId + 1;
        int parentId = currId / 2;

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

        if (isValidParent(parentId)){
            this.parent = parentId;
            this.parentPort = ServerConstants.BASE_PORT * ServerContext.getServer().getReplicaId() + this.parent;
        }
    }

    /**
     * Create payload for pushing up the tree
     * @param ts - timestamp to push
     * @return - payload for pushing up the tree
     */

    private String createPayloadForPushUp(Timestamp ts) {
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

    private Timestamp getMinVVTimestamp() {
        Timestamp[] currVV = ServerContext.getServer().getVersionVector();
        Timestamp currMin = currVV[0];

        for (int i = 1; i < currVV.length; i++) {
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
                Timestamp minLST = (leftLST.compareTo(rightLST) <= 0) ? leftLST : rightLST;
                assert minLST != null;
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
                Timestamp minLST = (leftLST.compareTo(rightLST) <= 0) ? leftLST : rightLST;
                assert minLST != null;
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
                forwardDown(msg);
                break;
        }
    }

    /**
     * Execute the aggregation service
     */

    public void run() {

        while (true) {
            String msg = this.pollMessage();
            processMessage(msg);
        }
    }

}
