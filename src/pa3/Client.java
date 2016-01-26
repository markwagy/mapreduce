package pa3;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Client is the class that the user interacts with. It contacts the Server 
 * for completing MapReduce operations.
 * @author mark
 */
public class Client {
    
    private String serverIP;
    private int serverPort;
    private Socket serverSocket;
    
    private static Logger log;

    /**
     * Client constructor
     * @param serverIP: IP address of the server
     * @param serverPort: port number of the server
     */
    public Client(String serverIP, String serverPort) {
        this.serverIP = serverIP;
        this.serverPort = Integer.valueOf(serverPort);
    }
    
    /**
     * main
     * args[0]: Server IP address
     * args[1]: Server port number
     */
    public static void main(String args[]) {
        // check params
        if (args.length < 2) {
            System.err.println("Usage: Client <serverIP> <serverPort>");
            System.exit(1);
        }
        PropertyConfigurator.configure("log4j.properties");
        log = Logger.getLogger(Client.class);
        log.info("Starting Client");
        Client client = new Client(args[0], args[1]);
        try {
            client.getServerSocket();
        } catch (UnknownHostException ex) {
            log.error("Unable to reach server, unknown host: " + ex);
        } catch (IOException ex) {
            log.error("Unable to reach server, IO problem: " + ex);
        }
        client.socialize();
    }
    
    /**
     * Get a connection with the Server
     */
    private void getServerSocket() throws UnknownHostException, IOException {
        if (serverIP == null || serverPort == -1 || serverPort == 0) {
            log.error("You have to set the server info before trying to contact it");
            System.exit(1);
        }
        NetworkLocation serverLocation = new NetworkLocation(serverIP, serverPort);
        serverSocket = new Socket(serverIP, serverPort);
        if (serverSocket == null) {
            log.error("Could not contact Server");
        }
    }
    
    /**
     * Talk to the user. Ask them what they want.
     */
    private void socialize() {
        println("Where is the file to sort? ");
        String fileName = readln();
        DataPackage dataPackage = loadFile(fileName);
        
        // keep track of time for reporting
        long startTime = System.currentTimeMillis();
        DataPackage sortedDataPackage = submitDataPackageForSort(dataPackage);
        long endTime = System.currentTimeMillis();
        log.info("Time for Map/Reduce sort: " + (endTime - startTime));
        
        if (sortedDataPackage != null) {
            println("Where would you like to write the sorted file?");
            fileName = readln();
            sortedDataPackage.writeToFile(fileName);
            println("File has been written to " + fileName);
        } else {
            log.error("Unsuccessul attempt at sorted data package!!");
        }
        println("Goodbye.");
    }
    
    /**
     * Sent a data package to the server to be sorted
     * @param dataPackage 
     */
    private DataPackage submitDataPackageForSort(DataPackage dataPackage) {
        if (serverSocket == null) {
            log.error("The Server has not been initialized properly");
        }
        {
            ObjectOutputStream oos = null;
            try {
                Protocol protocol = new Protocol(Protocol.Type.SORTDATA);
                oos = new ObjectOutputStream(serverSocket.getOutputStream());
                // send protocol type
                oos.writeObject(protocol);
                oos.flush();
                // send datapackage to sort
                oos.writeObject(dataPackage);
                oos.flush();
                ObjectInputStream ois 
                        = new ObjectInputStream(serverSocket.getInputStream());
                DataPackage sortedDataPackage = (DataPackage) ois.readObject();
                if (sortedDataPackage == null) {
                    log.error("Data package was not sorted!!");
                }
                return sortedDataPackage;
            } catch (ClassNotFoundException ex) {
                log.error("Class not found: " + ex);
            } catch (IOException ex) {
                log.error("IO Exception: "+ ex);
            } finally {
                try {
                    oos.close();
                } catch (IOException ex) {
                    log.error("IO Exception: " + ex);
                }
            }
        }
        log.error("Unable to sort data package. Returning unsorted version!!");
        return dataPackage;
    }
    
    /**
     * Load a requested file for sorting into a DataPackage object
     */
    private DataPackage loadFile(String fileName) {
        DataPackage dataPackage = new DataPackage();
        dataPackage.addFromFile(fileName);
        return dataPackage;
    }
    
    /**
     * method to cut down on typing (too much Lisping lately, I guess)
     */
    private static void println(String line) {
        System.out.println(line);
    }
    
    /**
     * Easy input method for simplicity
     */
    private static String readln() {
        // print prompt
        System.out.println(">> ");
        // get line
        Scanner input = new Scanner(System.in);
        String val = input.nextLine();
        return val;
    }
}
