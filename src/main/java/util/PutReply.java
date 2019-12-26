package util;

public class PutReply {

    private Timestamp dependencyTime;

    public PutReply(Timestamp dependencyTime) {
        this.dependencyTime = dependencyTime;
    }

    public Timestamp getDependencyTime() {
        return this.dependencyTime;
    }

}
