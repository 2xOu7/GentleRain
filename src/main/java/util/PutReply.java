package util;

import com.google.gson.Gson;

public class PutReply extends JSONSerializable {

    private Timestamp dependencyTime;

    public PutReply(Timestamp dependencyTime) {
        this.dependencyTime = dependencyTime;
    }

    public Timestamp getDependencyTime() {
        return this.dependencyTime;
    }

    public PutReply(String serializedForm) {
        PutReply pr = new Gson().fromJson(serializedForm, PutReply.class);
        this.dependencyTime = pr.getDependencyTime();
    }
}
