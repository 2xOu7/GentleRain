package server;

/**
 * Runtime context for the server class
 * Includes all of the relevant objects - getters + setters
 */

public class ServerContext {

    private static Server server; // the current server instance

    private static GSTAggregator gstAggregator; // the current GST Aggregator instance

    public static GSTAggregator getGstAggregator() {
        return gstAggregator;
    }

    private static LeafPusher leafPusher;

    private static HeartbeatReceiver heartbeatReceiver;

    private static HeartbeatPusher heartbeatPusher;

    public static Server getServer() {
        return server;
    }

    public static void setGstAggregator(GSTAggregator gstAggregator) {
        ServerContext.gstAggregator = gstAggregator;
    }

    public static void setServer(Server server) {
        ServerContext.server = server;
    }

    public static void setLeafPusher(LeafPusher leafPusher) {
        ServerContext.leafPusher = leafPusher;
    }

    public static LeafPusher getLeafPusher() {
        return leafPusher;
    }

    public static HeartbeatPusher getHeartbeatPusher() {
        return heartbeatPusher;
    }

    public static HeartbeatReceiver getHeartbeatReceiver() {
        return heartbeatReceiver;
    }

    public static void setHeartbeatPusher(HeartbeatPusher heartbeatPusher) {
        ServerContext.heartbeatPusher = heartbeatPusher;
    }

    public static void setHeartbeatReceiver(HeartbeatReceiver heartbeatReceiver) {
        ServerContext.heartbeatReceiver = heartbeatReceiver;
    }
}
