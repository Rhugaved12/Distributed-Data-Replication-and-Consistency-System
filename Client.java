import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Random;

public class Client {
    int clock = 0;
    int pid;

    public static void main(String[] args) throws IOException {
        if (args.length != 1 || Integer.parseInt(args[0]) < 0 || Integer.parseInt(args[0]) >= 4) {
            System.err.println("Usage: java Client <which pid to use [0-3]>");
            System.exit(1);
        }
        Client client = new Client();
        client.pid = Integer.parseInt(args[0]);
        client.startClient();
    }

    private void startClient() throws IOException {
        // List of server addresses and ports
        String[] hostNames = { "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
                "127.0.0.1" };
        int[] ports = { 6001, 6002, 6003, 6004, 6005, 6006, 6007 };

        // Create a socket to connect to the selected server
        Socket socket = null;
        PrintWriter out = null;
        BufferedReader in = null;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        String objectName = "object2";
        String wait = stdIn.readLine();
        try {
            Random random = new Random();
            int msgNo = 0;
            while (msgNo < 100) {
                if (msgNo % 25 == 0) {
                    System.err.println("Waiting for input: ");
                    wait = stdIn.readLine();
                }
                if (msgNo % 5 == 0)
                    objectName = "object5";
                else if (msgNo % 3 == 0)
                    objectName = "object3";
                else
                    objectName = "object1";

                Thread.sleep(random.nextInt(50));
                msgNo++;
                // Connect to one of the servers based on the DJB2 hash of the input string
                // System.err.println(
                // "Input any string to randomly select a server: (Enter 'Exit' to close the
                // current connection and start again)");
                // String userInput = stdIn.readLine();
                String userInput = Integer.toString(this.pid) + " value " + Integer.toString(msgNo);

                // Ask user for read or write
                // System.out.println("Do you want to read or write a message? (Enter 'read' or
                // 'write')");
                // String operation = stdIn.readLine();
                String operation = "write";

                // For any operation, let the client connect to any of the 3 replicas and let
                // the replicas figure out if they can connect with at least 1 more server when
                // writing/inserting. For read, just need to connect with 1 server
                boolean connected = false;
                for (int i = 0; i <= 4; i = i + 2) {
                    int serverIndex = (Math.abs(djb2Hash(objectName)) + i) % 7; // Select a server based on DJB2 hash
                    System.err.println("Server Hashed Index: " + serverIndex);
                    String serverAddress = hostNames[serverIndex];
                    int serverPort = ports[serverIndex];
                    try {
                        socket = new Socket(serverAddress, serverPort);
                        out = new PrintWriter(socket.getOutputStream(), true);
                        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        System.out.println("Connected to server " + serverAddress + ":" + serverPort);

                        // Send messages to the server based on user's choice
                        String message;
                        if (operation.equals("write")) {
                            message = createWriteMessage(stdIn, userInput, objectName);
                        } else if (operation.equals("read")) {
                            message = createReadMessage(stdIn);
                        } else {
                            System.err.println("Invalid operation. Please enter 'read' or 'write'.");
                            continue;
                        }

                        // if (i == 0) {
                        // message = "Test";
                        // }
                        out.println(message);
                        System.out.println("Message sent to server: " + message);

                        // Receive and print server response
                        String serverResponse = in.readLine();
                        System.out.println("Server response: " + serverResponse);

                        if (serverResponse.equals("Error")) {
                            System.err.println("Connecting with the next replica...");
                        } else {
                            if (operation.equals("write")) {
                                clock++;
                            }
                            connected = true;
                            break;
                        }
                    } catch (ConnectException e) {
                        // Server connection failed, try next server
                        System.err.println("Connection to server " + serverAddress + ":" + serverPort + " failed.");
                    }
                }
                if (!connected) {
                    System.err.println("Unable to connect to any server. Skipping message.");
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println("InterruptedException: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close resources
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (stdIn != null) {
                stdIn.close();
            }
            if (socket != null) {
                socket.close();
            }
        }
    }

    public String createWriteMessage(BufferedReader stdIn, String content, String objectName) throws IOException {
        // System.out.println("Enter object name:");
        // String objectName = stdIn.readLine();

        return "CLIENT&" + pid + "#W#" + clock + "#" + objectName + "#" + content;
    }

    public String createReadMessage(BufferedReader stdIn) throws IOException {
        // System.out.println("Enter object name:");
        // String objectName = stdIn.readLine();
        String objectName = "object";
        return "CLIENT&" + pid + "#R#" + clock + "#" + objectName;
    }

    // DJB2 hash function implementation
    public static int djb2Hash(String str) {
        int hash = 5381;
        for (int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) + hash) + str.charAt(i); // hash * 33 + char
        }
        return hash;
    }
}
