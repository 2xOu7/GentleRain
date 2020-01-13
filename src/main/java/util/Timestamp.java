package util;

import com.google.gson.Gson;

public class Timestamp extends JSONSerializable implements Comparable<Timestamp> {

    private long physicalClockTime;
    private int replicaId;
    private int partitionId;
    private static String delimiter = "@";

    /**
     * Creates a timestamp with the time at instantiation as the clockTime (JVM clock)
     */

    public Timestamp() {
        this.physicalClockTime = System.nanoTime();
    }

    public Timestamp(String serializedForm) {
        Timestamp curr = new Gson().fromJson(serializedForm, Timestamp.class);
        this.physicalClockTime = curr.getPhysicalClockTime();
        this.replicaId = curr.getReplicaId();
        this.partitionId = curr.getPartitionId();

    }

    public Timestamp(int replicaId, int partitionId) {
        this.physicalClockTime = System.nanoTime();
        this.replicaId = replicaId;
        this.partitionId = partitionId;
    }

    public Timestamp replicaId(int replicaId) {
        this.replicaId = replicaId;
        return this;
    }

    public Timestamp timestamp(long ts) {
        this.physicalClockTime = ts;
        return this;
    }

    public Timestamp partitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public long getPhysicalClockTime() {
        return physicalClockTime;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int compareTo(Timestamp o) {

        if (this.physicalClockTime < o.physicalClockTime) {
            return -1;
        }

        if (this.physicalClockTime > o.physicalClockTime) {
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
