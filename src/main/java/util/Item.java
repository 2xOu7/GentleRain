package util;

public class Item {
    private String key;
    private String value;
    private Timestamp updateTime;
    private int sourceReplica;
    private String delimiter = "<";

    public Item(String key, String value, Timestamp ut, int sourceReplica) {
        this.key = key;
        this.value = value;
        this.updateTime = ut;
        this.sourceReplica = sourceReplica;
    }

    public Item(String serializedForm) {
        String[] tokens = serializedForm.split("<");
        this.key = tokens[0];
        this.value = tokens[1];
        this.updateTime = new Timestamp(tokens[2]);
        this.sourceReplica = Integer.parseInt(tokens[3]);
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

    @Override
    public String toString() {
        String[] tokens = new String[4];

        tokens[0] = this.key;
        tokens[1] = this.value;
        tokens[2] = this.updateTime.toString();
        tokens[3] = Integer.toString(this.sourceReplica);

        return String.join(this.delimiter, tokens);
    }
}
