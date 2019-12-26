package server;

/**
 * This thread will try to aggregate the minimum LST among its children nodes
 * It will push the result to the parent node
 * When the parent node pushes the result back down, this thread will then update the GST of the server
 */

public class GSTAggregation {

    public Server server;

    public Integer parent;
    public Integer leftChild;
    public Integer rightChild;

    private boolean isValidChild(int id) {
        return id <= server.getNumReplicas();
    }

    private boolean isValidParent(int id) {
        return id > 0;
    }

    public GSTAggregation(Server server) {
        this.server = server;

        int currId = server.getPartitionId();
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

}
