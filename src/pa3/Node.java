package pa3;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import java.net.Socket;
import java.util.logging.Level;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


/**
 * Node class
 * Compute node in the system.
 * @author mark
 */
public class Node extends Thread {
    
    private static Logger log;
    
    // probability that this node will force itself to fail per reqs
    private double failureProbability; 
    private double loadThreshold;
    private NetworkLocation location;

    private NodeStats stats;
    private ServerSocket ss;
    private NetworkLocation serverLocation;
    
    public static String NODE_RMI_DESC = "Node";
    public static final String FAILURE_PROB_VARNAME = "FailureProbability";
    public static final String LOAD_THRESHOLD_VARNAME = "LoadThreshold";
    // as per the requirements, we need to write out to intermediate sorted file
    public static final String INTERMEDIATE_FILENAME = "intermediate.sorted";
    
    /**
     * Available functions by remote calls
     */
    public static enum PROTOCOL {Sort, SortForPeer, GetStats};
    
    /**
     * Node constructor
     */
    public Node(NetworkLocation serverLocation) {
        this(0.0, 1.0, serverLocation);
    }
    
    public Node(double failureProbability, double loadThreshold, 
            NetworkLocation serverLocation) {
        this.failureProbability = failureProbability;
        this.loadThreshold = loadThreshold;
        stats = new NodeStats(location);
        this.serverLocation = serverLocation;
    }
    
    /**
     * main class
     * @param args 
     */
    public static void main(String args[]) {
        // check input
        if (args.length < 2) {
            System.err.append("Usage: Node <server IP> <server port>");
            System.exit(1);
        }
        PropertyConfigurator.configure("log4j.properties");
        log = Logger.getLogger(Node.class);

        double failureProbability = 0.0;
        double loadThreshold = 1.0;
        
        // the optional third argument is a failure probability
        if (args.length==3) {
            failureProbability = Double.valueOf(args[2]);
        }
        

        // get failure probability and load threshold from config file
        try {
            PropertiesConfiguration config = 
                    new PropertiesConfiguration("node.properties");
            failureProbability = config.getDouble(FAILURE_PROB_VARNAME);
            loadThreshold = config.getDouble(LOAD_THRESHOLD_VARNAME);
        } catch (ConfigurationException ex) {
            log.error("Problem loading properties config file: " + ex);
        }
                
        log.info("Starting Node with a failure probability of " + 
                failureProbability + " and load threshold of " + 
                loadThreshold + " ...");
        NetworkLocation serverLocation = new NetworkLocation(args[0], args[1]);        
        Node node = new Node(failureProbability, loadThreshold, serverLocation);
        node.start();

    }


    /*
     * Overriden run method for thread
     */
    @Override
    public void run() {
        init();
        try {
            log.info("Started node at " + getLocation());
            while (true) {
                log.debug("listening for requests...");
                Socket socket = ss.accept();
                log.debug("got request");
                dispatch(socket);
            }
        } catch (ClassNotFoundException ex) {
            log.error("Class not found: " + ex);
        } catch (IOException ex) {
            log.error("IO Exception: " + ex);
        }
    }
    
    /**
     * Dispatch according to protocol
     */
    private void dispatch(Socket socket) throws IOException, ClassNotFoundException {
        log.debug("dispatching according to protocol");
        
        ObjectInputStream ois 
                = new ObjectInputStream(socket.getInputStream());
        
        // get the protocol to follow
        Protocol protocol = (Protocol) ois.readObject();
        log.debug("using protocol: " + protocol);
        
        if (protocol.getType().equals(Protocol.Type.SORTDATA)) {
            log.info("got request to sort");
            log.debug("reading input DP");
            DataPackage inDP = (DataPackage) ois.readObject();
            log.debug("reading node list");
            ArrayList<NetworkLocation> nodeList 
                    = (ArrayList<NetworkLocation>) ois.readObject();
            log.debug("sorting rows");
            DataPackage outDP = sortRows(inDP, nodeList);
            ObjectOutputStream oos 
                    = new ObjectOutputStream(socket.getOutputStream());
            log.debug("writing sorted DP");
            oos.writeObject(outDP);
            oos.flush();
        } else if (protocol.getType().equals(Protocol.Type.SORTFORPEER)) {
            log.info("got request to sort for peer");
            log.debug("reading input DP");
            DataPackage inDP = (DataPackage) ois.readObject();
            log.debug("sorting rows");
            DataPackage outDP = sortRowsForPeer(inDP);
            ObjectOutputStream oos
                    = new ObjectOutputStream(socket.getOutputStream());
            log.debug("writing sorted DP");
            oos.writeObject(outDP);
            oos.flush();
        } else if (protocol.getType().equals(Protocol.Type.STATS)) {
            log.info("got a request for stats");
            ObjectOutputStream oos 
                    = new ObjectOutputStream(socket.getOutputStream());
            log.debug("writing stats");
            oos.writeObject(stats);
            oos.flush();
        } else if (protocol.getType().equals(Protocol.Type.REDUCE)) {
            // TDOO:
            log.info("got request to reduce");
            log.debug("reading DP partitions");
            ArrayList<DataPackage> dpPartitions 
                    = (ArrayList<DataPackage>) ois.readObject();
            ArrayList<DataRow> outRows = reduce(dpPartitions);
            ObjectOutputStream oos 
                    = new ObjectOutputStream(socket.getOutputStream());
            log.debug("writing reduced rows");
            oos.writeObject(outRows);
            oos.flush();
        } else {
            log.error("Unknown protocol: " + protocol);
        }
        log.debug("done");
    }
    
    /**
     * Initialize this node - 
     * Set up a connection to the Server and tell it of this node's presence
     * so that it can give some work.
     */
    private void init() {
        try {
            ss = new ServerSocket(0);
            int port = ss.getLocalPort();
            // get my ip address
            String ip = Utils.getIPAddress();
            // create NetworkLocation with this info
            location = new NetworkLocation(ip, port);

            // add node's info to server list
            Protocol protocol = new Protocol(Protocol.Type.NODEJOIN);
            Socket socket = new Socket(serverLocation.ip, serverLocation.port);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            log.debug("writing protocol");
            oos.writeObject(protocol);
            oos.flush();
            log.debug("writing location");
            oos.writeObject(this.location);
            oos.flush();
        } catch (IOException ex) {
            log.error("IO exception: " + ex);
        }
    }
    
    
    /**
     * Get this node's port number
     */
    public int getPort() {
        return this.location.port;
    }

    /**
     * Get this node's IP address
     */
    public String getIP() {
        return this.location.ip;
    }
    
    /**
     * sort function to be called by Server
     */
    public DataPackage sortRows(DataPackage inDP, ArrayList<NetworkLocation> nodeList) {

        /**
         * Check system load against load threshold
         */
        double systemLoad = getSystemLoad();
        
        /*
         * update stats
         */
        stats.currentLoad = systemLoad;
        // assure no divide by zeroes
        if (stats.numberOfMigrations != 0) {
            stats.averageLoad = stats.averageLoad 
                    + systemLoad/stats.numberOfMigrations;
        }
        
        log.info("Current system load = " + systemLoad);
        log.info("Load threshold = " + loadThreshold);
        boolean passToOthers = systemLoad > loadThreshold;
        if (passToOthers && nodeList.size()>1) {
            log.info("We will try to pass the work on to others");
            for (NetworkLocation netLocation : nodeList) {
                // don't try myself
                if (!location.equals(netLocation)) {
                    DataPackage outDP = tryPeerForSort(inDP, netLocation);
                    if (outDP != null) {
                        stats.numberOfMigrations++;
                        log.info("Node at " + netLocation + 
                                " taking over for peer with too much load");
                        return outDP;
                    }
                }
            }
        } else if (nodeList.size() <=1) {
            log.info("Not enough peers to pass work on, sorting by myself");
        } else {
            log.info("We have enough resources, continuing with sort");
        }
        
        return sortRows(inDP);
    }
    
    /*
     * Function to get a peer to attempt a sort if current node has too much load
     * returns sorted DP if peer was able, otherwise returns null
     */
    private DataPackage tryPeerForSort(DataPackage inDP, NetworkLocation n) {
        try {
            DataPackage outDP = null;

                log.debug("requesting peer (" + n + ") to sort");
                Socket peerSocket = new Socket(n.ip, n.port);
                Protocol protocol = new Protocol(Protocol.Type.SORTFORPEER);
                ObjectOutputStream oos
                        = new ObjectOutputStream(peerSocket.getOutputStream());
                log.debug("sending protocol");
                oos.writeObject(protocol);
                oos.flush();
                log.debug("sending data package");
                oos.writeObject(inDP);
                oos.flush();
                log.debug("getting sorted DP");
                ObjectInputStream ois 
                        = new ObjectInputStream(peerSocket.getInputStream());
                outDP = (DataPackage) ois.readObject();
                return outDP;
        } catch (ClassNotFoundException ex) {
            log.error("class not found: " + ex);
        } catch (UnknownHostException ex) {
            log.error("unknown host: " + ex);
        } catch (IOException ex) {
            log.error("io exception: " + ex);
        }
        return null;
    }
    
    /**
     * sort function to be called by other nodes when they have too much load
     */
    public DataPackage sortRowsForPeer(DataPackage inDP) {
        // update stats
        stats.numberOfTasksSeen++;
        
        /* 
         * If this node can manage, sort. otherwise return null
         */
        double systemLoad = getSystemLoad();
        if (loadThreshold > systemLoad) {
            return sortRows(inDP);
        } 
        return null;
    }

    /**
     * sort the incoming rows
     */
    public DataPackage sortRows(DataPackage inDP) {
        /**
         * Introduce failure per reqs.
         * Pick a random number from 0 to 1 and if this number is over
         * the "failure probability, then fail this sort operation 
         * (return null)
         */
        Random rand = new Random();
        double randVal = rand.nextDouble();
        // failure probability of 0.0 should be no failures
        if (randVal > 1.0 - this.failureProbability) {
            log.info("(Forced) Node failure at " + 
                    this.location + " during sort.");
            stats.numberOfFaults++;
            return null;
        }
        // if the node gets here, it didn't fail
        ArrayList<DataRow> rows = inDP.getRows();
        Collections.sort(rows);
        DataPackage outDP = new DataPackage(rows);
        // set sorted to true to show this DP sort was a success
        outDP.sorted(true); 
        if (!outDP.isSorted()) {
            log.fatal(
                    "This should never happen - " +
                    "we are assuming that the DP is sorted here");
        }
        
        /**
         * write DP out to intermediate file as per reqs
         */
        outDP.writeToFile(INTERMEDIATE_FILENAME);
        
        // now return the actual sorted data package
        return outDP;
    }
    
    /**
     * Get current system load
     */
    private double getSystemLoad() {
        final OperatingSystemMXBean osBean 
                = ManagementFactory.getOperatingSystemMXBean();
        double load = osBean.getSystemLoadAverage();
        log.info("Load: " + load);
        return load;
    }

    /**
     * Getter for node location
     */
    private NetworkLocation getLocation() {
        return this.location;
    }
    
    /**
     * reduce functionality
     */
    public ArrayList<DataRow> reduce(ArrayList<DataPackage> partitions) {
        
        // return list
        ArrayList<DataRow> reducedRows = new ArrayList<DataRow>();
        
        int numPartitions = partitions.size();
        
        // throw all of the rows in the first rowset into the reduced rows
        reducedRows.addAll(partitions.get(0).getRows());
        
        // then merge successive rowsets into existing one (if they exist)
        for (int partitionNum=1; partitionNum < numPartitions; partitionNum++) {
            ArrayList<DataRow> currentPartitionRows 
                    = partitions.get(partitionNum).getRows();
                    
            reducedRows = merge(reducedRows, currentPartitionRows);
        }
        
        return reducedRows;
    }
    
    /**
     * Merge two sets of rows that are each *sorted* already
     */
    private ArrayList<DataRow> merge(ArrayList<DataRow> le, ArrayList<DataRow> ri) {
        // create return list
        ArrayList<DataRow> mergedRows = new ArrayList<DataRow>();
        
        // this is the index into the mergedRows array list
        int mergedIdx = 0;
        // indices into the "left" and "right" rows
        int idxL = 0;
        int idxR = 0;
        
        while (mergedIdx < le.size() + ri.size()) {
            if (idxR >= ri.size()) {
                // we've exhausted all values in the right rows, 
                // so just add the rest of the left
                mergedRows.addAll(Utils.getRest(idxL, le));
                break;
            } else if (idxL >= le.size()) {
                // we've exhausted all values in the left rows,
                // so add the rest of the right
                //mergedRows.addAll(idxR, ri);
                mergedRows.addAll(Utils.getRest(idxR, ri));
                break;
            } else {
                DataRow riRow = ri.get(idxR);
                DataRow leRow = le.get(idxL);
                
                // if left row is smaller than right row
                if (leRow.compareTo(riRow) < 0) {
                    // insert it into merged rows
                    mergedRows.add(leRow);
                    // increment the merged index
                    mergedIdx++;
                    // and increment the left index (to compare with exists, bigger, right val)
                    idxL++;
                } else {
                    // reversed
                    mergedRows.add(riRow);
                    mergedIdx++;
                    idxR++;
                }
            }
        }

        return mergedRows;
    }
    
    /**
     * RMI call to Get this node's stats
     */
    public NodeStats getStats() {
        return stats;
    }
    
}
