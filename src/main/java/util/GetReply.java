package util;

public class GetReply {

    private String key;
    private Timestamp updateTime;
    private Timestamp globalStableTime;

    public GetReply(String key, Timestamp updateTime, Timestamp globalStableTime) {
        this.key = key;
        this.updateTime = updateTime;
        this.globalStableTime = globalStableTime;
    }

    public String getKey() {
        return key;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public Timestamp getGlobalStableTime() {
        return globalStableTime;
    }
}
