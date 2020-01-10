package server;
import kong.unirest.Unirest;
import util.Timestamp;

/**
 * Will periodically push the this server's heartbeat out to other servers
 */

public class HeartbeatPusher extends Thread {

    private long sleepTime;
    private long startTime;
    private int replicaId;
    private int partitionId;
    private String messageAddress;

    public HeartbeatPusher() {
        this.sleepTime = 5000;
        this.startTime = System.nanoTime();
        this.replicaId = ServerContext.getServer().getReplicaId();
        this.partitionId = ServerContext.getServer().getPartitionId();
        this.messageAddress = "http://localhost:{port}/heartbeat/{payload}";
    }

    /**
     * Will sleep for next iteration
     */

    private void waitForNextRound() {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns whether or not it is valid to push the heartbeat out
     * @param currTime - current time
     * @param delta - delta
     * @return - whether or not it is valid to push the heartbeat out
     */

    private boolean isValidToChangeVV(long currTime, long delta) {
        Timestamp[] currVV = ServerContext.getServer().getVersionVector();
        return currTime >= currVV[this.replicaId].getClockTime() + delta;
    }

    /**
     * Broadcasts heartbeat to all other servers
     */

    private void broadcastHeartbeat() {
        int M = ServerContext.getServer().getNumReplicas();

        for (int i = 1; i <= M; i++) {

            if (i == this.replicaId) {
                continue;
            }

            sendHeartbeat(i);

        }
    }

    /**
     * Sends a heartbeat to replica i with the same partition number
     * @param i - the replica to send to
     */

    private void sendHeartbeat(int i) {
        int portToForward = ServerConstants.BASE_PORT * i + partitionId;
        String payload = ServerContext.getServer().getVersionVector()[this.replicaId].toString();

        try {

            Unirest.put(this.messageAddress)
                    .routeParam("port", Integer.toString(portToForward))
                    .routeParam("payload", payload)
                    .asString();

        } catch (Exception ignored) {

        }
    }

    @Override
    public void run() {

        long lastTime = startTime;

        while (true) {
            long currTime = System.nanoTime();
            long delta = currTime - lastTime;

            if (isValidToChangeVV(currTime, delta)) {

                Timestamp newTS = new Timestamp()
                        .timestamp(currTime)
                        .partitionId(this.partitionId)
                        .replicaId(this.replicaId);

                ServerContext.getServer().setVersionVector(this.replicaId, newTS);
                broadcastHeartbeat();
            }

            lastTime = currTime;
            waitForNextRound();
        }
    }
}
