package util;

import com.google.gson.Gson;

public class GetReply extends JSONSerializable {

    private String key;
    private Timestamp updateTime;
    private Timestamp globalStableTime;

    public GetReply(String key, Timestamp updateTime, Timestamp globalStableTime) {
        this.key = key;
        this.updateTime = updateTime;
        this.globalStableTime = globalStableTime;
    }

    public GetReply(String serializedForm) {
        GetReply gr = new Gson().fromJson(serializedForm, GetReply.class);
        this.key = gr.getKey();
        this.updateTime = gr.getUpdateTime();
        this.globalStableTime = gr.getGlobalStableTime();
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
