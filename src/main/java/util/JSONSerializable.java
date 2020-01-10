package util;

import com.google.gson.Gson;

public class JSONSerializable {

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
