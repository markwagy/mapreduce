package pa3;

import java.io.Serializable;

/**
 * NetworkLocation
 * Simple class to house IP address and port info
 * @author mark
 */
public class NetworkLocation implements Serializable {
    public String ip;
    public int port;
    
    public NetworkLocation(String ip, String port) {
        this.ip = ip;
        this.port = Integer.valueOf(port);
    }
    
    public NetworkLocation(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String toString() {
        return "[" + ip + ":" + port + "]";
    }
}
