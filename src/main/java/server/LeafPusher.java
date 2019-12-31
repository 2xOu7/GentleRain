package server;

import util.Timestamp;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Will occasionally push the minimum LST up the tree
 */

public class LeafPusher extends Thread {

    private ReentrantLock msgLock = new ReentrantLock();
    private Condition waitForNextRound = msgLock.newCondition();
    private boolean roundInSession = false;

    private GSTAggregator currGSTAggregator;

    /**
     * Starts the next session
     */

    public void startNextSession() {

        try {
            Thread.sleep(10000); // just for testing right now
            this.msgLock.lock();
            this.roundInSession = false;
            this.waitForNextRound.signal();

        } catch (InterruptedException e) {
            e.printStackTrace();

        } finally {
            this.msgLock.unlock();
        }
    }

    private void pushMinLST() {
        try {
            this.msgLock.lock();

            while (roundInSession) {
                this.waitForNextRound.await();
            }

            roundInSession = true;
            Timestamp minTS = this.currGSTAggregator.getMinVVTimestamp();
//            ServerContext.getServer().getLogger().logPrint("Min VV: " + minTS);
            String payload = this.currGSTAggregator.createPayloadForPushUp(minTS);
            this.currGSTAggregator.addMessage(payload);

        } catch (InterruptedException e) {
            e.printStackTrace();

        } finally {
            this.msgLock.unlock();
        }
    }

    @Override
    public void run() {
        this.currGSTAggregator = ServerContext.getGstAggregator();

        while (true) {
            pushMinLST();
        }
    }
}
