package pa3;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


/**
 * Server class
 * Distributes operations to the Nodes and serves as an access point 
 * to use with the Client.
 * @author mark
 */
public class Server extends Thread {
    
    private static Logger log;
    private ArrayList<NetworkLocation> nodeList;
    private NetworkLocation location;
    private ServerStats stats;
    
    ServerSocket ss;
    
    public static String SERVER_RMI_DESC = "Server";
    // this is the number of times the server will try to gather a set of
    // working nodes for sort before giving up
    public static final int NUM_TRIES = 5;
    
    /**
     * Server constructor
     */
    public Server() {
        nodeList = new ArrayList<NetworkLocation>();
        stats = new ServerStats();
    }
    
    /**
     * main
     * @param args: no args needed
     */
    public static void main(String args[]) {
        PropertyConfigurator.configure("log4j.properties");
        log = Logger.getLogger(Client.class); 
        log.info("Starting Server...");
        Server server = new Server();
        server.start();
    }
    
    /**
     * Thread run method
     */
    @Override
    public void run() {
        try {
            init();
            log.info("Server running at: " + getLocation());
            while (true) {
                Socket socket = ss.accept();
                dispatch(socket);
            }
        } catch (ClassNotFoundException ex) {
            log.error("Could not find object class: " + ex);
        } catch (IOException ex) {
            log.error("Unable to create server socket: " + ex);
            System.exit(1);
        }
    }
    
    /*
     * Metho to dispatch functionality according to a Protocol
     * must cast return object based on expected behavior
     */
    private void dispatch(Socket socket) 
            throws IOException, ClassNotFoundException {
        ObjectInputStream ois 
                = new ObjectInputStream(socket.getInputStream());
        
        // get the protocol to follow
        Protocol protocol = (Protocol) ois.readObject();
        
        if (protocol.getType().equals(Protocol.Type.SORTDATA)) {
            try {
                log.info("Recieved request to sort");
                // get input DP from socket
                DataPackage inDP = (DataPackage) ois.readObject();
                DataPackage outDP = acceptDataPackageForSort(inDP);
                ObjectOutputStream oos 
                        = new ObjectOutputStream(socket.getOutputStream());
                // write output DP to socket
                oos.writeObject(outDP);
                oos.flush();
                log.info("wrote out DP");
            } catch (IOException ex) {
                log.error(
                        "IO Exception when trying to dispatch acceptDataPackageForSort: " 
                        + ex);
            } catch (ClassNotFoundException ex) {
                log.error(
                        "Class Not Found when trying to dispatch acceptDataPackageForSort: " 
                        + ex);
            }
        } else if (protocol.getType().equals(Protocol.Type.NODEJOIN)) {
            try {
                log.info("Received request to join");
                NetworkLocation nodeLocation = (NetworkLocation) ois.readObject();
                addNodeInfo(nodeLocation);
                log.info("Node joined");
            } catch (IOException ex) {
                log.error(
                        "IO Exception when trying to dispatch addNodeInfo: " 
                        + ex);
            } catch (ClassNotFoundException ex) {
                log.error(
                        "Class Not Found when trying to dispatch addNodeInfo: "
                        + ex);
            }
        } else {
            log.error("Unknown protocol: " + protocol);
        }
    }


    /**
     * Accept a data package for sort, returns sorted data package
     */
    public DataPackage acceptDataPackageForSort(DataPackage dataPackage) {
        log.info("Received data package");
        int numTries = Server.NUM_TRIES;
//        ArrayList<Socket> nodes = gatherNodes();

        // increment request stat
        stats.numberOfSortRequestsSinceStartup++;
        
        /**
         * try to map and reduce multiple times 
         * this is so that, in case nodes are down, we gather new sets of 
         * nodes multiple times in hopes of finding a stable set
         */
        for (int tryNumber = 0; tryNumber < numTries; tryNumber++) {
            try {
                ArrayList<DataPackage> sortedPartitions = 
                        new ArrayList<DataPackage>(nodeList.size());
                        
                /* IN: unsorted data packages, all nodes available */
                sortedPartitions = map(dataPackage, nodeList);
                /* OUT: individually sorted data packages */
                
                /*
                 * if we've gotten here, all is well and we can reduce
                 * and return the data package
                 */
                DataPackage sortedDataPackage = 
                        reduce(sortedPartitions);
                
                /**
                 * print stats here
                 */
                gatherStats();
                log.info(stats);
                
                return sortedDataPackage;
            } catch (NullPointerException nex) {
                // if we've gotten a null pointer exception, it's because nodes are all dead
                log.error(
                        "All nodes have died, please try rerunning when valid nodes are available");
                break;
            } catch (ArithmeticException aex) {
                // this means we've divided by zero, zero being the number of 
                // available nodes
                log.error(
                       "All nodes are down, please try rerunning when nodes are available");
            }
        }
        
        // if we get here, we haven't been able to complete map/reduce successfully
        log.error("Unable to map/reduce!!");
        
        return null;
    }
    
    /**
     * Mapper function that splits tasks into chunks for nodes to analyze
     * Returns a data package that needs to be merged
     */
    private ArrayList<DataPackage> map(DataPackage dp, ArrayList<NetworkLocation> nodes) {

        int numNodes = nodes.size();
        ArrayList<DataRow> unsortedRows = dp.getRows();
        
        // var to keep track of how many nodes went down
        int badNodeCount = 0;
        
        // structure to store each partition of sorted data to be merged
        ArrayList<DataPackage> unsortedPartitions = 
                new ArrayList<DataPackage>(numNodes);
        
        // structure to place partitions of sorted data (to be merged)
        ArrayList<DataPackage> sortedPartitions = 
                new ArrayList<DataPackage>(numNodes);
        
        // get partitions and store in unsorted DP partitions
        for (int nodeIndex=0; nodeIndex < numNodes; nodeIndex++) {
            DataPackage currDP = getPartition(nodeIndex, numNodes, unsortedRows);
            unsortedPartitions.add(currDP);
        }
        
        int numberOfPartitionsToSort = unsortedPartitions.size();
        // while there are still partitions to sort keep trying to sort them
        while (numberOfPartitionsToSort > 0) {
            // for each node, attempt to send a data package for sort
            for (   int nodeIndex=0; 
                    nodeIndex<numNodes && numberOfPartitionsToSort > 0; 
                    nodeIndex++) {
                ObjectOutputStream oos = null;
                try {

                    // get current node
                    NetworkLocation currNodeLoc = nodes.get(nodeIndex);
                    Socket currentNode = new Socket(currNodeLoc.ip, currNodeLoc.port);
                    
                    // get a partition from unsorted ones - pull from the "bottom"
                    DataPackage currentUnsortedPartition
                            = unsortedPartitions.get(0);
                    // get sorted data rows for this partition
                    Protocol sortProtocol = new Protocol(Protocol.Type.SORTDATA);
                    oos = new ObjectOutputStream(currentNode.getOutputStream());
                    log.debug("requesting sort at " + currNodeLoc);
                    oos.writeObject(sortProtocol);
                    oos.flush();
                    log.debug("sending DP partition");
                    oos.writeObject(currentUnsortedPartition);
                    oos.flush();
                    log.debug("writing node list");
                    oos.writeObject(nodeList);
                    oos.flush();
                    log.debug("getting sorted DP partition");
                    ObjectInputStream ois 
                            = new ObjectInputStream(currentNode.getInputStream());
                    log.debug("got object output stream");
                    DataPackage sortedDP =
                            (DataPackage) ois.readObject();
                    log.debug("got sorted DP partition");
                    if (sortedDP == null || (!sortedDP.isSorted())) {
                        // a node was down, give this DP to someone else
                        log.debug("node went down, giving this DP to someone else");
                        badNodeCount++;
                        continue;
                    } else {
                        // all is well, add to the list of sorted DPs
                        log.debug("successfully sorted. adding to sorted partitions");
                        sortedPartitions.add(sortedDP);
                        unsortedPartitions.remove(currentUnsortedPartition);
                        numberOfPartitionsToSort = unsortedPartitions.size();
                        log.debug("number of partitions left to sort" 
                                + numberOfPartitionsToSort);
                    }
                } catch (ClassNotFoundException ex) {
                    log.error("class not found: " + ex);
                } catch (IOException ex) {
                    log.error("io exception: " + ex);
                } finally {
                    try {
                        log.debug("closing object output stream");
                        oos.close();
                    } catch (IOException ex) {
                        log.error("io exception: " + ex);
                    }
                }
            }
        }

        return sortedPartitions;
    }
    
    /**
     * Get a partition of total rows
     */
    private DataPackage getPartition (
            int partitionNumber, int numberOfPartitions, ArrayList<DataRow> rows) {
        int totalNumberOfRows = rows.size();
        int maxNumRowsPerPartition = (int) totalNumberOfRows/numberOfPartitions;
        log.info("Splitting data into " + numberOfPartitions + " partitions.");
        log.info("Each partition will contain at most " + 
                maxNumRowsPerPartition + " rows");
        
        /*
         * get upper and lower bounds on the indices where the partition on the 
         * data rows are 
         */
        int lowerBoundIndex = partitionNumber*maxNumRowsPerPartition;
        int upperBoundIndex = lowerBoundIndex + maxNumRowsPerPartition;
        
        /* if upper bound is over the total number of rows 
         * (which it will be for the last partition)
         * then assign it to the total number of rows-1 (the last row)
         */
        upperBoundIndex = (upperBoundIndex > totalNumberOfRows-1)?totalNumberOfRows:upperBoundIndex;
        
        // create new arraylist of rows to store partition of data rows
        DataRow[] rowsPartition = new DataRow[maxNumRowsPerPartition];
        
        // currentRow is the index into the insert row number
        int currentRow = 0;
        
        // from the lower bound index of the partition of rows to the upper bound
        // gatther the data rows to return
        for (int i=lowerBoundIndex; i<upperBoundIndex; i++) {
            rowsPartition[currentRow] = rows.get(i);
            // increment the current row index
            currentRow++;
        }
        
        return new DataPackage(rowsPartition);
    }
    
    /**
     * Reduce function
     */
    private DataPackage reduce(ArrayList<DataPackage> partitions) {
        try {
            // get a node index to do reduce on
            int nodeIndex = fetchRandomNodeIndex();
            // get its network location and the node instance
            NetworkLocation nodeLoc = nodeList.get(nodeIndex);
            Socket socket = new Socket(nodeLoc.ip, nodeLoc.port);
            
            Protocol reduceProtocol = new Protocol(Protocol.Type.REDUCE);
            
            ObjectOutputStream oos
                    = new ObjectOutputStream(socket.getOutputStream());
            log.debug("requesting " + nodeLoc + " to reduce");
            oos.writeObject(reduceProtocol);
            oos.flush();
            log.debug("sending partitions");
            oos.writeObject(partitions);
            oos.flush();
            ObjectInputStream ois
                    = new ObjectInputStream(socket.getInputStream());
            log.debug("getting reduced rows");
            ArrayList<DataRow> reducedRows = (ArrayList<DataRow>) ois.readObject();
            // create new sorted data package after reducing from reduce node
            DataPackage sortedDataPackage = new DataPackage(reducedRows);
            return sortedDataPackage;
        } catch (ClassNotFoundException ex) {
            log.error("class not found: " + ex);
        } catch (UnknownHostException ex) {
            log.error("unknown host: " + ex);
        } catch (IOException ex) {
            log.error("io exception: " + ex);
        }
        log.error("error, returning null");
        return null;
    }

    /**
     * Gather nodes that are still up
     */
    private ArrayList<Socket> gatherNodes() {
        ArrayList <Socket> nodes = new ArrayList<Socket>();

        for (NetworkLocation n : nodeList) {
            try {
                Socket node;

                node = new Socket(n.ip, n.port);
                    
                if (node != null) {
                    log.info("Adding node at " + n + " to nodes used for sorting.");
                    nodes.add(node);
                } else {
                    log.warn("Node is down at " + n);
                }
            } catch (UnknownHostException ex) {
                log.error("unknown host: " + ex);
            } catch (IOException ex) {
                log.error("io exception: " + ex);
            }
        }
        
        return nodes;
    }

    /**
     * Called by each node as it starts up to make the Server aware that it is
     * available for work
     */
    public void addNodeInfo(NetworkLocation nodeLocation) {
        nodeList.add(nodeLocation);
    }

    private void init() throws IOException {
                // get an available port
        ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        // get my ip address
        String ip = Utils.getIPAddress();
        // create NetworkLocation with this info
        location = new NetworkLocation(ip, port);
    }
    
    int getPort() {
        return location.port;
    }
    
    String getIP() {
        return location.ip;
    }
    
    NetworkLocation getLocation() {
        return location;
    }
    
    /**
     * fetch a random node (for reduce operation for example)
     */
    int fetchRandomNodeIndex() {
        Random r = new Random();
        int randInt = r.nextInt(this.nodeList.size());
        return randInt;
    }

    /**
     * RMI method Get server statistics
     */
    public ServerStats getStats() {
        /*
         * need to gather stats of all nodes to have something 
         * interesting for the server to report
         */
        gatherStats();
        return stats;
    }
    
    /**
     * Gather stats from all nodes
     */
    private void gatherStats() {
        ArrayList<Socket> nodes = gatherNodes();
        ArrayList<NodeStats> ns = new ArrayList<NodeStats>();
        Protocol statsProtocol = new Protocol(Protocol.Type.STATS);
        for (Socket n : nodes) {
            ObjectOutputStream oos = null;
            try {
                oos = new ObjectOutputStream(n.getOutputStream());
                log.debug("request stats");
                oos.writeObject(statsProtocol);
                oos.flush();
                ObjectInputStream ois
                        = new ObjectInputStream(n.getInputStream());
                log.debug("get stats");
                NodeStats stats = (NodeStats) ois.readObject();
                log.debug("add to list");
                ns.add(stats);
            } catch (ClassNotFoundException ex) {
                log.error("class not found: " + ex);
            } catch (IOException ex) {
                log.error("io exception: " + ex);
            } finally {
                try {
                    oos.close();
                } catch (IOException ex) {
                    log.error("io exception: " + ex);
                }
            }

        }
        stats.setNodeStats(ns);
    }
}
