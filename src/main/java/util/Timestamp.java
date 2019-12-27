package util;

import java.sql.Time;

public class Timestamp implements Comparable<Timestamp> {

    private long clockTime;
    private int replicaId;
    private int partitionId;
    private static String delimiter = "@";

    /**
     * Creates a timestamp with the time at instantiation as the clockTime (JVM clock)
     */

    public Timestamp() {
        this.clockTime = System.nanoTime();
    }

    public Timestamp(String serializedForm) {
        String[] tokens = serializedForm.split(delimiter);

        this.clockTime = Long.parseLong(tokens[0]);
        this.replicaId = Integer.parseInt(tokens[1]);
        this.partitionId = Integer.parseInt(tokens[2]);
    }

    public Timestamp(int replicaId, int partitionId) {
        this.clockTime = System.nanoTime();
        this.replicaId = replicaId;
        this.partitionId = partitionId;
    }

    public Timestamp replicaId(int replicaId) {
        this.replicaId = replicaId;
        return this;
    }

    public Timestamp timestamp(long ts) {
        this.clockTime = ts;
        return this;
    }

    public Timestamp partitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public long getClockTime() {
        return clockTime;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        String[] tokens = new String[3];

        tokens[0] = Long.toString(this.clockTime);
        tokens[1] = Integer.toString(this.replicaId);
        tokens[2] = Integer.toString(this.partitionId);

        return String.join(delimiter, tokens);
    }

    @Override
    public int compareTo(Timestamp o) {

        if (this.clockTime < o.clockTime) {
            return -1;
        }

        if (this.clockTime > o.clockTime) {
            return 1;
        }

        if (this.replicaId < o.replicaId) {
            return -1;
        }

        if (this.replicaId > o.replicaId) {
            return 1;
        }

        return Integer.compare(this.partitionId, o.partitionId);

    }

}
