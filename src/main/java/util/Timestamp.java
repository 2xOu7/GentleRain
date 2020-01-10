package util;

import com.google.gson.Gson;

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
        Timestamp curr = new Gson().fromJson(serializedForm, Timestamp.class);
        this.clockTime = curr.getClockTime();
        this.replicaId = curr.getReplicaId();
        this.partitionId = curr.getPartitionId();

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
        return new Gson().toJson(this);
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
