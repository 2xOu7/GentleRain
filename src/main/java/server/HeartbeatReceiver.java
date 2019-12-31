package server;

import com.google.gson.Gson;
import util.MessageBox;
import util.Timestamp;

/**
 * Receiver of heartbeats from other servers
 */

public class HeartbeatReceiver extends MessageBox {

    /**
     * Processes a message by changing the version vector to the time from the new timestamp
     * @param msg - the timestamp sent over
     */

    public void processMessage(String msg) {
        Timestamp ts = new Gson().fromJson(msg, Timestamp.class);
        ServerContext.getServer().setVersionVector(ts.getReplicaId(), ts);
    }

    @Override
    public void run() {

        while (true) {
            String msg = this.pollMessage();
            processMessage(msg);
        }
    }
}
