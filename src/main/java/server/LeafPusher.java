package server;

import util.Timestamp;

/**
 * Will occasionally push the minimum LST up the tree
 */

public class LeafPusher extends Thread {

    /**
     * Stops the thread for 10 seconds before restarting next round
     */

    public void waitForNextRound() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        GSTAggregator currGSTAggregator = ServerContext.getGstAggregator();

        while (true) {
            Timestamp minTS = currGSTAggregator.getMinVVTimestamp();
            String payload = currGSTAggregator.createPayloadForPushUp(minTS);
            currGSTAggregator.addMessage(payload);

            waitForNextRound();
        }
    }
}
