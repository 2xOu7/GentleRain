package server;

public class Logger {

    private Server server;

    private static boolean toPrint = true;
    public Logger(Server s) {
        this.server = s;
    }

    public void logPrint(String s) {
        if (!toPrint) {
            return;
        }

        System.out.println("Server in partition " +
                this.server.getPartitionId() +
                " and data center " +
                this.server.getReplicaId() + ": " + s);
    }
}
