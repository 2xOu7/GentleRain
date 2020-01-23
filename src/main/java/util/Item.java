package util;

import com.google.gson.Gson;

public class Item extends JSONSerializable {

    private String key;
    private String value;
    private Timestamp updateTime;
    private int sourceReplica;

    public Item(String key, String value, Timestamp ut, int sourceReplica) {
        this.key = key;
        this.value = value;
        this.updateTime = ut;
        this.sourceReplica = sourceReplica;
    }

    public Item(String serializedForm) {
        Item d = new Gson().fromJson(serializedForm, Item.class);
        this.key = d.getKey();
        this.value = d.getValue();
        this.updateTime = d.getUpdateTime();
        this.sourceReplica = d.getSourceReplica();
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public int getSourceReplica() {
        return sourceReplica;
    }
}
