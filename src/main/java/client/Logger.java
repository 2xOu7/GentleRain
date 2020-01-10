package client;

public class Logger {

    private Client client;

    private static boolean toPrint = true;

    public Logger(Client s) {
        this.client = s;
    }

    public void logPrint(String s) {
        if (!toPrint) {
            return;
        }

        System.out.println("Client for replica " +
                this.client.getReplicaId() + ": " + s);
    }
}
