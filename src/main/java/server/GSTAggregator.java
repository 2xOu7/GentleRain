package server;

import kong.unirest.Unirest;
import util.MessageBox;
import util.Timestamp;

import java.sql.Time;

/**
 * This thread will try to aggregate the minimum LST among its children nodes
 * It will push the result to the parent node
 * When the parent node pushes the result back down, this thread will then update the GST of the server
 */

public class GSTAggregator extends MessageBox {

    public Server server; // server of this aggregator

    public Integer parent; // partitionId of parent
    public Integer leftChild; // partitionId of left child
    public Integer rightChild; // partitionId of right child

    private boolean isValidChild(int id) {
        return id <= server.getNumReplicas();
    }

    private boolean isValidParent(int id) {
        return id > 0;
    }

    public GSTAggregator() {
        int currId = ServerContext.getServer().getPartitionId();
        int leftId = 2 * currId;
        int rightId = 2 * currId + 1;
        int parentId = currId / 2;

        if (isValidChild(leftId)) {
            this.leftChild = leftId;
        }

        if (isValidChild(rightId)) {
            this.rightChild = rightId;
        }

        if (isValidParent(parentId)){
            this.parent = parentId;
        }
    }

    /**
     * Pushes a message to the parent node
     * @param msg - the message to be pushed
     */

    private void pushMessage(String msg) {
        int portToForward = ServerConstants.BASE_PORT * this.server.getReplicaId() + this.parent;
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
                pushMessage(msg);
                break;

            case GLOBAL_MIN_LST: // this is the time that is pushed down - we set this server's GST to this value
                Timestamp currTs = new Timestamp(tokens[2]);
                this.server.setGlobalStableTime(currTs);
                break;
        }
    }

    public void run() {

        while (true) {
            String msg = this.pollMessage();
            processMessage(msg);
        }
    }

}
