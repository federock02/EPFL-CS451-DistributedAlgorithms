package cs451;

import java.io.File;

public class ConfigParser {

    private String path;

    // parsing configuration file path
    public boolean populate(String value) {
        File file = new File(value);
        path = file.getPath();
        return true;
    }

    public String getPath() {
        return path;
    }

}
