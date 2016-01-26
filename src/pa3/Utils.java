package pa3;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import org.apache.log4j.Logger;

public class Utils {
    private static Logger log;

    /**
     * Get RMI reference to the Server
     */
    public static ServerImpl getServerInstance(NetworkLocation serverLocation) {
        log = Logger.getLogger(Utils.class);
        
        ServerImpl server = null;
        
        try {
            Registry registry =
                    LocateRegistry.getRegistry(serverLocation.ip, serverLocation.port);
            try {
                server =
                        (ServerImpl) registry.lookup(Server.SERVER_RMI_DESC);
                return server;
            } catch (NotBoundException ex1) {
                log.error("Couldn't lookup registry because it isn't bound: "
                        + ex1);
            } catch (AccessException ex1) {
                log.error("Couldn't lookup registry due to lack of access: "
                        + ex1);
            }
        } catch (RemoteException e) {
            log.error("Remote exception: " + e);
        }
        if (server == null) {
            log.fatal("Unable to contact the Server!");
        }
        return null;
    }
    
    /**
     * Get RMI reference to the Node instance
     */
    public static NodeImpl getNodeInstance(NetworkLocation nodeLocation) throws RemoteException {
        log = Logger.getLogger(Utils.class);
        NodeImpl node;
        
        Registry registry =
                LocateRegistry.getRegistry(nodeLocation.ip, nodeLocation.port);
        try {
            node =
                    (NodeImpl) registry.lookup(Node.NODE_RMI_DESC);
            return node;
        } catch (NotBoundException ex1) {
            log.error("Couldn't lookup registry because it isn't bound: "
                    + ex1);
        } catch (AccessException ex1) {
            log.error("Couldn't lookup registry due to lack of access: "
                    + ex1);
        }

        return null;
    }
    
    /**
     * Get this machine's IP address
     */
    public static String getIPAddress() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostAddress();
        } catch (UnknownHostException e) {
            log.error("I can't find my own ip address: " + e);
            return null;
        }
    }
    
    /**
     * Get values of arraylist from offset to end
     */
    public static <T> ArrayList<T> getRest (int offset, ArrayList<T> li) {
        ArrayList<T> returnList = new ArrayList<T>();
        for (int idx=offset; idx < li.size(); idx++) {
            returnList.add(li.get(idx));
        }
        return returnList;
    }
}
