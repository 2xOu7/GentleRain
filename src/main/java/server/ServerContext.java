package server;

/**
 * Runtime context for the server class
 */

public class ServerContext {

    private static Server server; // the current server instance

    private static GSTAggregator gstAggregator; // the current GST Aggregator instance

    public static GSTAggregator getGstAggregator() {
        return gstAggregator;
    }

    private static LeafPusher leafPusher;

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
}
