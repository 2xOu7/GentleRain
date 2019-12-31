package server;

public class ServerConstants {
    public static String SERVER_GET_PATH = "/get/*/*";
    public static String SERVER_PUT_PATH = "/put/*/*/*";
    public static String SERVER_REPLICATE_PATH = "/replicate/*/*";
    public static String SERVER_LST_AGGREGATE_PATH = "/aggregate/:payload";
    public static String SERVER_HEARTBEAT_PATH = "/heartbeat/:payload";
    public static String PAYLOAD_PARAM = ":payload";
    public static int BASE_PORT = 10000;
}
