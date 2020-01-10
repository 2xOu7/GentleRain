package server;

import com.google.gson.Gson;
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

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public AggregationMessage(String serializedForm) {
        AggregationMessage am = new Gson().fromJson(serializedForm, AggregationMessage.class);
        this.aggregationEnum = am.getAggregationEnum();
        this.senderId = am.getSenderId();
        this.timestamp = am.getTimestamp();
    }
}
