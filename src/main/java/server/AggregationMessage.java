package server;

import util.Timestamp;

public class AggregationMessage {

    private int senderId;
    private Timestamp timestamp;
    private AggregationEnum aggregationEnum;

    public AggregationMessage(AggregationEnum ae, int senderId, Timestamp ts) {
        this.senderId = senderId;
        this.timestamp = ts;
        this.aggregationEnum = ae;
    }


    public int getSenderId() {
        return senderId;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public AggregationEnum getAggregationEnum() {
        return aggregationEnum;
    }
}
