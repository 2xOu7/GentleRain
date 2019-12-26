package util;

public class PutRequest {

    private String key;
    private String value;
    private Timestamp dependencyTime;

    public PutRequest(String key, String value, Timestamp dependencyTime) {
        this.key = key;
        this.value = value;
        this.dependencyTime = dependencyTime;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }

    public Timestamp getDependencyTime() {
        return this.dependencyTime;
    }

}
