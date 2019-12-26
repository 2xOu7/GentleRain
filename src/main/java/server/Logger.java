package server;

public class Logger {

    private Server server;

    public Logger(Server s) {
        this.server = s;
    }

    public void logPrint(String s) {
        System.out.println("Server in partition " +
                this.server.getPartitionId() +
                " and data center " +
                this.server.getReplicaId() + ": " + s);
    }
}
