import java.net.*;
import java.security.spec.ECGenParameterSpec;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.time.LocalTime;
import java.io.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {

    // Define class-level variables for hostNames, ports, and pid
    private static String[] hostNames = { "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
            "127.0.0.1" };
    private static int[] ports = { 6001, 6002, 6003, 6004, 6005, 6006, 6007 };
    private static int pid;

    // Define a boolean array to store the status of each server
    static boolean[] serverStatus = new boolean[7]; // Assuming 7 servers

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1 && Integer.parseInt(args[0]) >= 0 && Integer.parseInt(args[0]) < 7) {
            System.err.println("Usage: java Server <which port number to use [0-3]>");
            System.exit(1);
        }
        pid = Integer.parseInt(args[0]); // Initialize pid
        int serverPortNumber = ports[pid];
        // Creating a process object which will be shared with all the server threads
        Process process = new Process(pid);
        // Load data from files if available
        process.loadDataFromFile();

        // Creating the server on a thread
        Thread server = new MultiThreadServer(serverPortNumber, pid, process);
        server.start();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        // Process waits for 10secs before broadcasting messages to each server
        Thread.sleep(10000);
        // Initial Connection with servers
        List<EchoClient> clients = new ArrayList<>();

        for (int i = 0; i < hostNames.length; i++) {
            if (i != pid) { // Assuming 'pid' is the ID of the current server
                EchoClient client = new EchoClient(i, serverStatus);
                try {
                    client.connect(hostNames[i], ports[i]);
                    clients.add(client); // Add the client to the list
                    serverStatus[i] = true; // Mark the server as "up"
                } catch (IOException e) {
                    // Handle connection failure
                    System.err.println("Failed to connect with Server " + i);
                    serverStatus[i] = false; // Mark the server as "down"
                }
            }
        }

        // System.out.println("List of EchoClients:");
        // for (EchoClient client : clients) {
        // System.out.println(client.pid);
        // }

        ClientProcess clientProcess = new ClientProcess(pid, clients);
        process.setConnectedServers(clientProcess);

        // For every server(other than self), set the clientProcess
        for (EchoClient client : clients) {
            client.setClientProcess(clientProcess);
        }

        // Method to update current server replica when restarted with missing data from
        // all other replicas.
        // We send all the other replicas of a particular object the current
        // server+object clock value and wait for return of
        // new messages (if any) at that replica. The current server then saves the
        // newer messages
        process.updateCurrentServerReplicaWithMissingData();

        // Start the periodic reconnection task
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> reconnectWithDownServers(clients), 0, 15, TimeUnit.SECONDS);

        LocalTime currentTime = LocalTime.now();
        // Test: Send messages to servers
        // for (EchoClient client : clients) {

        // client.sendMessage("SERVER&" + Integer.toString(client.clientProcess.myPID) +
        // "test"
        // + Integer.toString(ports[client.clientProcess.myPID]) + " " + currentTime);
        // }

    }

    public static void reconnectWithDownServers(List<EchoClient> clients) {
        System.err.println("Trying to reconnect");
        for (int i = 0; i < serverStatus.length; i++) {
            System.out.print("Server " + i + " status: " + serverStatus[i] + ", ");
        }
        System.err.println();

        for (int i = 0; i < hostNames.length; i++) {
            if (i != pid && !serverStatus[i]) { // Check if the server is not the current server and is marked as down
                // Attempt connection with server i
                System.err.println("Trying to reconnect with server: " + i);
                try {
                    String hostName = hostNames[i];
                    int portNumber = ports[i];
                    EchoClient client = new EchoClient(i, serverStatus);
                    client.connect(hostName, portNumber);
                    System.out.println("[reconnectWithDownServers] Pid: " + client.pid + " i: " + i);
                    // If connection successful, update server status and replace the existing
                    // client in the list
                    serverStatus[i] = true;
                    // Replace the existing client in the list with the new client
                    int index = i;
                    if (i >= pid) {
                        index -= 1;
                    }
                    clients.set(index, client);

                    System.out.println("Reconnected with Server " + index);
                } catch (IOException e) {
                    // Handle connection failure
                    System.err.println("IOException: Failed to reconnect with Server " + i + ": " + e.getMessage());
                    e.printStackTrace();
                    // Optionally, you can log the exception or perform additional actions
                } catch (Exception e) {
                    System.err.println("Failed to reconnect with Server " + i + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

}

class Process {
    public int clock = 0;
    private int pid;
    public ClientProcess connectedServers;

    // Map to store messages from clients
    private Map<String, List<String>> clientMessages = new HashMap<>();

    // Map to store queued messages for each object name
    private Map<String, PriorityQueue<String>> queuedMessages = new HashMap<>();

    // Map to store clocks for each object
    public Map<String, Integer> objectClocks = new HashMap<>();

    public Process(int pid) {
        this.pid = pid; // pid takes values between 0 to 3
    }

    public void setConnectedServers(ClientProcess clientProcess) {
        this.connectedServers = clientProcess;
    }

    public synchronized void updateCurrentServerReplicaWithMissingData() {
        for (Map.Entry<String, Integer> entry : this.objectClocks.entrySet()) {
            String objectName = entry.getKey();
            int clockValue = entry.getValue();
            System.err.println("[updateCurrentServerReplicaWithMissingData] Objectname: " + objectName + "clockValue: "
                    + clockValue);

            int serverIndex = Math.abs(djb2Hash(objectName)) % 7;
            System.err.println("Sending Update Replica message to other servers: ServerIndex: " + serverIndex);

            String handshakeMessage = "HANDSHAKE&" + Integer.toString(this.pid) + "#" + Integer.toString(clockValue)
                    + "#" + objectName;

            System.err.println("Handshake message: " + handshakeMessage);

            for (int i = 0; i < this.connectedServers.clients.size(); i++) {
                int index = i;
                if (i >= this.pid) {
                    index++;
                }
                EchoClient client = this.connectedServers.clients.get(i);
                // send message to other 2 replicas. We don't know which replica current server
                // is wrt to serverIndex
                System.err.println("Server Index:: " + serverIndex + ", i: " + i + ", index: " + index
                        + ", client.pid: " + client.pid);
                if ((client.pid == serverIndex) || (client.pid == (serverIndex + 2) % 7)
                        || (client.pid == (serverIndex + 4) % 7)) {
                    System.err.println("Sending Message to : " + client.pid + " with index: " + index);
                    boolean reply = client.sendMessage(handshakeMessage);
                    if (reply == true) {
                        System.err.println("Handshake Message Delivered Successfully");
                    } else {
                        System.err.println("Handshake Message Could Not Be Delivered");
                    }
                }
            }

        }
    }

    public synchronized void processMessage(PrintWriter out, String msg) throws IOException {
        try {
            System.err.println("[processMessage] Received MSG: " + msg + " " + this.pid);
            // if (this.pid == 3) {
            // this.connectedServers.clients.get(5).out.println("HELLO ");
            // }
            // Split the message based on "&" to differentiate between client and server
            // messages
            String[] parts = msg.split("&");
            // if (parts.length != 2) {
            // // Handle invalid message format
            // System.err.println("Invalid message format: " + msg);
            // return;
            // }

            String messageType = parts[0];
            String content = parts[1];

            // Depending on whether it's a client or server message, call different
            // handlers/processors
            if (messageType.equals("CLIENT")) {
                processClientMessage(out, content);
            } else if (messageType.equals("SERVER")) {
                processServerMessage(out, content);
            } else if (messageType.equals(("HANDSHAKE"))) {
                processHandshakeMessage(out, content);
            } else {
                // Handle invalid message type or mismatch between message type and connection
                // type
                System.err.println("Invalid message type or mismatch: " + messageType);
            }

            // Save data to files after processing the message
            saveDataToFile();

        } catch (Exception e) {
            System.err.println("[processMessage] Exception: " + e.getMessage());
            out.println("Error");
        }

        // out.println(msg); // Echo the message back to the sender
    }

    // Process the handshake message received
    public synchronized void processHandshakeMessage(PrintWriter out, String content) {
        try {
            String[] parts = content.split("#");
            if (parts.length == 3) {
                int senderServerId = Integer.parseInt(parts[0]);
                int receivedClockValue = Integer.parseInt(parts[1]);
                String objectName = parts[2];

                // Example: Print the parsed values
                System.out.println("Handshake message from Server ID " + senderServerId + ":");
                System.out.println("Received Clock Value: " + receivedClockValue);
                System.out.println("Object Name: " + objectName);

                // Get the current clock value for the object
                int currentClockValue = objectClocks.getOrDefault(objectName, -1);
                System.err.println("Current Clock Value: " + currentClockValue);

                // If the current clock value is smaller or equal to the received clock value,
                // return
                if (currentClockValue <= receivedClockValue) {
                    System.out.println("Current clock value for object " + objectName
                            + " is smaller or equal to received clock value." + " Current Clock Value: "
                            + currentClockValue + " Received Clock Value: " + receivedClockValue);
                    out.println("ACK");
                    return;
                }

                // If the current clock value is larger than the received clock value, send
                // missing messages
                System.out.println("Continuing Processing...Current clock value for object " + objectName
                        + " is larger than received clock value. Sending missing messages...");

                // Make sure there is connection between this server and the handshaking server
                // A connection in which this server acts as client and that server acts as
                // server
                if (Server.serverStatus[senderServerId] != false) {
                    Server.serverStatus[senderServerId] = false;
                    Server.reconnectWithDownServers(this.connectedServers.clients);
                }

                // Iterate through the stored messages for the object
                List<String> messages = getClientMessages(objectName);
                int handshakeReplySuccessCount = 0;
                for (String msg : messages) {
                    String[] msgParts = msg.split("#");
                    String msgContent = msgParts[0];
                    int msgClockValue = Integer.parseInt(msgParts[1]);

                    // Check if the message clock value is between the received and current clock
                    // values
                    if (msgClockValue > receivedClockValue) {
                        // Send the message to the server requesting handshake
                        String handshakeReplyMessage = "SERVER&" + Integer.toString(this.pid) + "#W#"
                                + Integer.toString(msgClockValue) + "#" + objectName + "#" + msgContent;
                        for (EchoClient client : this.connectedServers.clients) {
                            if (client.pid == senderServerId) {
                                System.err.println("Sending Handshake Reply Message to : " + client.pid
                                        + " Reply Message: " + handshakeReplyMessage);
                                boolean reply = client.sendMessage(handshakeReplyMessage);
                                if (reply == true) {
                                    handshakeReplySuccessCount++;
                                    System.err.println("Handshake Reply Delivered Successfully");
                                } else {
                                    System.err.println("Handshake Reply Not Delivered");
                                }
                            } else {
                                System.err.println(
                                        "Client ID: " + client.pid
                                                + " Not Sending Handshake Reply as different Client");
                            }
                        }
                    }
                }
                System.err.println("handshakeReplySuccessCount: " + handshakeReplySuccessCount
                        + "(currentClockValue - receivedClockValue)"
                        + (Integer.toString((currentClockValue - receivedClockValue))));
                // After sending all newer messages back as Handshake Reply
                if (handshakeReplySuccessCount == (currentClockValue - receivedClockValue)) {
                    System.err.println("Sending ACK as SuccessCount Reached");
                    out.println("ACK");
                } else {
                    System.err.println("Sending Error as SuccessCount Not Reached");
                    out.println("Error");
                }
            } else {
                // If the handshake message format is incorrect, send "Error"
                System.err.println("[processHandshakeMessage]: Error: Invalid message format");
                out.println("Error: Invalid message format");
            }
        } catch (Exception e) {
            // If any exception occurs during message processing, send "Error"
            System.err.println("Error processing handshake message: " + e.getMessage());
            e.printStackTrace();
            out.println("Error");
        }
    }

    // Process client message
    public synchronized void processClientMessage(PrintWriter out, String content) throws IOException {
        try {
            String[] parts = content.split("#");
            if (parts[1].equals("W")) {
                int clientPid = Integer.parseInt(parts[0]);
                int clockValue = Integer.parseInt(parts[2]);
                String objectName = parts[3];
                String messageContent = parts[4];

                // Example: Print the parsed values
                System.out.println("Client message from PID " + clientPid + ":");
                System.out.println("Clock Value: " + clockValue);
                System.out.println("Object Name: " + objectName);
                System.out.println("Message Content: " + messageContent);

                // Update the clock value for the object after storing the message
                int newClockValue = objectClocks.getOrDefault(objectName, -1);
                // Increment clockvalue first as the clock value in objectClocks is of the last
                // saved message.
                // When saving the objectClocks in file, we increment and save bringing it at
                // the same level as the latest saved message
                newClockValue++;
                String serverMessage = "SERVER&" + Integer.toString(this.pid) + "#W#" + Integer.toString(newClockValue)
                        + "#" + objectName
                        + "#" + messageContent;
                System.err.println("ServerMessage: " + serverMessage);
                // Send messages only to 2 other replicas
                int serverIndex = Math.abs(djb2Hash(objectName)) % 7;
                int noOfReplicaReplies = 0;
                System.err.println("Sending Client message to other servers: " + serverIndex);
                for (int i = 0; i < this.connectedServers.clients.size(); i++) {
                    int index = i;
                    if (i >= this.pid) {
                        index++;
                    }
                    EchoClient client = this.connectedServers.clients.get(i);
                    if ((index == (serverIndex + 2) % 7) || (index == (serverIndex + 4) % 7)) {
                        System.err.println("Sending Message to : " + client.pid + " with index: " + index);
                        boolean reply = client.sendMessage(serverMessage);
                        if (reply == true) {
                            noOfReplicaReplies++;
                        }
                    }
                }

                if (noOfReplicaReplies > 0) {
                    // Successfully saved the message at atleast 1 other replica + saving the
                    // message on this server = Total 2 replicas
                    // Store the message in the server using objectClocks
                    storeClientMessage(objectName, messageContent);

                    // Retrieve stored messages for the objectName
                    List<String> storedMessagesList = getClientMessages(objectName);
                    String storedMessagesString = convertClientMessagesAsString(storedMessagesList);

                    // Send only the stored messages content back to the client
                    out.println(storedMessagesString);
                } else {
                    System.err.println("Couldn't save the message on other replica. Not saving the message");
                    out.println("Error");
                }

            } else if (parts[1].equals("R")) {
                int clientPid = Integer.parseInt(parts[0]);
                int clockValue = Integer.parseInt(parts[2]);
                String objectName = parts[3];

                // Example: Print the parsed values
                System.out.println("Client message from PID " + clientPid + ":");
                System.out.println("Clock Value: " + clockValue);
                System.out.println("Object Name: " + objectName);

                // Retrieve stored messages for the objectName
                String storedMessages = getClientMessagesAsString(objectName);
                if (storedMessages.isEmpty()) {
                    out.println("Error");
                } else {
                    out.println(storedMessages); // Send only the stored messages content
                }

            } else {
                out.println("Error");
            }
        } catch (Exception e) {
            System.err.println("[handleClientMessages] Exception: " + e.getMessage());
            e.printStackTrace();
            out.println("Error");
        }
    }

    // Store client message with objectName as key, along with the clock value
    private synchronized void storeClientMessage(String objectName, String messageContent) {
        // Check if the objectName already exists in the map
        if (!clientMessages.containsKey(objectName)) {
            // If not, create a new list for the objectName
            clientMessages.put(objectName, new ArrayList<>());
            // Set the initial clock value for the object to 0
            objectClocks.put(objectName, -1);
        }
        // Get the current clock value for the object
        int clockValue = objectClocks.get(objectName);
        // Increment the clock value
        clockValue++;
        // Update the clock value for the object
        objectClocks.put(objectName, clockValue);
        // Add the message content and updated clock value to the list of messages for
        // the objectName
        String messageWithClock = messageContent + "#" + clockValue; // Append clock value to the message
        clientMessages.get(objectName).add(messageWithClock);
    }

    // Method to retrieve messages for a specific objectName
    public synchronized List<String> getClientMessages(String objectName) {
        return clientMessages.getOrDefault(objectName, new ArrayList<>());
    }

    // Method to retrieve messages for a specific objectName
    public synchronized String getClientMessagesAsString(String objectName) {
        List<String> messages = clientMessages.getOrDefault(objectName, new ArrayList<>());
        StringBuilder result = new StringBuilder();
        for (String message : messages) {
            result.append(message).append("  ");
        }
        return result.toString();
    }

    // Method to convert messages to a string
    public synchronized String convertClientMessagesAsString(List<String> messages) {
        StringBuilder result = new StringBuilder();
        for (String message : messages) {
            result.append(message).append("  ");
        }
        return result.toString();
    }

    // Process server message
    public synchronized void processServerMessage(PrintWriter out, String content) {
        System.err.println("[processServerMessage] Server Message: " + content);

        // Parse the content string
        // Format: for Write: pid#W#Clock_Value#Object_name#content
        try {
            String[] parts = content.split("#");
            if (parts[1].equals("W")) {
                int clientPid = Integer.parseInt(parts[0]);
                int clockValue = Integer.parseInt(parts[2]);
                String objectName = parts[3];
                String messageContent = parts[4];

                int lastClockValue = objectClocks.getOrDefault(objectName, -1); // Get last clock value for the object
                // If the received message clock is equal to or less than the current server
                // clock, skip processing
                if (clockValue <= lastClockValue) {
                    System.err.println("Received message clock for object " + objectName
                            + " is equal to or less than the current server clock. Skipping processing.");
                    out.println("ACK"); // Send acknowledgment to the sender
                    return;
                }

                // Example: Print the parsed values
                System.out.println("Client message from PID " + clientPid + ":");
                System.out.println("Clock Value: " + clockValue);
                System.out.println("Object Name: " + objectName);
                System.out.println("Message Content: " + messageContent);

                // // Get stored client messages in the given object
                // List<String> previouslyStoredClientMessagesList =
                // getClientMessages(objectName);

                // String lastStoredMessage = null; // Initialize to null

                // if (!previouslyStoredClientMessagesList.isEmpty()) {
                // lastStoredMessage = previouslyStoredClientMessagesList
                // .get(previouslyStoredClientMessagesList.size() - 1);
                // }

                // Check delivery eligibility for current message using objectClocks
                if (clockValue == lastClockValue + 1) {
                    // Message can be delivered
                    // Store the message in the server using updated method
                    storeClientMessage(objectName, messageContent);
                    System.err.println("Stored Current Message: " + clockValue + " " + messageContent);
                    // After delivery of current message check if other messages with same
                    // objectName can be delivered
                    List<String> listOfMessagesThatCanBeDelivered = getListOfMessagesThatCanBeDelivered(objectName,
                            lastClockValue);

                    for (String msg : listOfMessagesThatCanBeDelivered) {
                        String[] msgParts = msg.split("#");
                        int msgClockValue = Integer.parseInt(msgParts[1]);
                        String msgContent = msgParts[0];
                        storeClientMessage(objectName, msgContent);
                        System.err.println("Storing message from Queue: " + msgClockValue + " " + msgContent);
                    }
                } else {
                    System.err.println(
                            "Queuing current Message: " + objectName + " " + messageContent + " " + clockValue);
                    queueCurrentMessage(objectName, messageContent, clockValue);
                }
                // Retrieve stored messages for the objectName
                String storedMessages = getClientMessagesAsString(objectName);
                System.err.println("Server Get MEsaages: ==== " + storedMessages);
                out.println("ACK"); // Send only the stored messages content
            }
        } catch (Exception e) {
            // If any exception occurs during message processing, send "Error"
            System.err.println("Error processing message: " + e.getMessage());
            out.println("Error");
        }
    }

    // Method to retrieve messages that can be delivered for a given object name
    public synchronized List<String> getListOfMessagesThatCanBeDelivered(String objectName, int lastClockValue) {
        List<String> messagesToDeliver = new ArrayList<>();

        // Check if there are queued messages for the given object name
        if (queuedMessages.containsKey(objectName)) {
            PriorityQueue<String> queue = queuedMessages.get(objectName);

            // Iterate through the queued messages
            while (!queue.isEmpty()) {
                String message = queue.peek();
                int clockValue = Integer.parseInt(message.split("#")[1]);

                // Check if the message can be delivered
                if (clockValue == lastClockValue + 1) {
                    // Message can be delivered
                    messagesToDeliver.add(queue.poll()); // Remove the message from the queue
                    lastClockValue++; // Update the last clock value
                } else {
                    // Message cannot be delivered yet, break the loop
                    break;
                }
            }
        }

        return messagesToDeliver;
    }

    // Method to queue a message that cannot be delivered yet
    private synchronized void queueCurrentMessage(String objectName, String messageContent, int clockValue) {
        // Check if there is already a queue for the given object name
        PriorityQueue<String> queue = queuedMessages.get(objectName);

        // If there is no queue for the object name, create a new one
        if (queue == null) {
            queue = new PriorityQueue<>((a, b) -> {
                int clockA = Integer.parseInt(a.split("#")[1]);
                int clockB = Integer.parseInt(b.split("#")[1]);
                return Integer.compare(clockA, clockB);
            });
            queuedMessages.put(objectName, queue);
        }

        // Add the message content and clock value to the priority queue
        queue.offer(messageContent + "#" + clockValue);
    }

    // Method to save data to files when server shuts down
    public synchronized void saveDataToFile() {
        try {
            Gson gson = new Gson();

            // Serialize and save clientMessages
            try (Writer writer = new FileWriter("clientMessages" + Integer.toString(this.pid) + ".json")) {
                gson.toJson(clientMessages, writer);
            }

            // Serialize and save queuedMessages
            try (Writer writer = new FileWriter("queuedMessages" + Integer.toString(this.pid) + ".json")) {
                gson.toJson(queuedMessages, writer);
            }

            // Serialize and save objectClocks
            try (Writer writer = new FileWriter("objectClocks" + Integer.toString(this.pid) + ".json")) {
                System.out.println("Loaded objectClocks from file: " + objectClocks);
                gson.toJson(objectClocks, writer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to load data from files when server starts up
    public synchronized void loadDataFromFile() {
        Gson gson = new Gson();

        // Load clientMessages from file
        File clientMessagesFile = new File("clientMessages" + this.pid + ".json");
        if (clientMessagesFile.exists()) {
            try (Reader reader = new FileReader(clientMessagesFile)) {
                clientMessages = gson.fromJson(reader, new TypeToken<Map<String, List<String>>>() {
                }.getType());
                System.out.println("Loaded clientMessages from file: " + clientMessages);
            } catch (IOException e) {
                System.err.println("Error reading clientMessages file: " + e.getMessage());
            }
        } else {
            createNewFile(clientMessagesFile);
        }

        // Load queuedMessages from file
        File queuedMessagesFile = new File("queuedMessages" + this.pid + ".json");
        if (queuedMessagesFile.exists()) {
            try (Reader reader = new FileReader(queuedMessagesFile)) {
                queuedMessages = gson.fromJson(reader, new TypeToken<Map<String, PriorityQueue<String>>>() {
                }.getType());
                System.out.println("Loaded queuedMessages from file: " + queuedMessages);
            } catch (IOException e) {
                System.err.println("Error reading queuedMessages file: " + e.getMessage());
            }
        } else {
            createNewFile(queuedMessagesFile);
        }

        // Load objectClocks from file
        File objectClocksFile = new File("objectClocks" + this.pid + ".json");
        if (objectClocksFile.exists()) {
            try (Reader reader = new FileReader(objectClocksFile)) {
                objectClocks = gson.fromJson(reader, new TypeToken<Map<String, Integer>>() {
                }.getType());
                System.out.println("Loaded objectClocks from file: " + objectClocks);
            } catch (IOException e) {
                System.err.println("Error reading objectClocks file: " + e.getMessage());
            }
        } else {
            createNewFile(objectClocksFile);
        }
    }

    private void createNewFile(File file) {
        try {
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.err.println("Error creating the file: " + e.getMessage());
        }
    }

    public static int djb2Hash(String str) {
        int hash = 5381;
        for (int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) + hash) + str.charAt(i); // hash * 33 + char
        }
        return hash;
    }

}

class ClientProcess {

    int myPID;
    List<EchoClient> clients;

    public ClientProcess(int pid, List<EchoClient> clients) {
        this.clients = clients;
        this.myPID = pid;
    }

    public synchronized void processStringAndStore(String input, EchoClient targetClient) throws IOException {

        // Attempt to send the message to the target client
        boolean success = targetClient.sendMessage(input);

        if (!success) {
            // Handle failure to send the message to the target client
            // You can log an error, retry, or take other appropriate actions
            // For now, let's just print an error message
            System.err.println("Failed to send message to target client.");
            // You might also throw an exception here depending on how you want to handle
            // the error
        }
    }
}

/// Class that handles sending of messages to other servers
// This class acts as the client side of the other server connections.
class EchoClient {
    private Socket echoSocket;
    PrintWriter out;
    private BufferedReader in;
    public ClientProcess clientProcess;
    // this is the PID of the Server that this server is connected to.
    int pid;

    private boolean[] serverStatus; // Array to store the status of each server

    public EchoClient(int pid, boolean[] serverStatus) {
        this.pid = pid;
        this.serverStatus = serverStatus;
    }

    public void setClientProcess(ClientProcess clientProcess) {
        this.clientProcess = clientProcess;
    }

    // Method to connect with the server
    public void connect(String hostName, int portNumber) throws IOException {
        System.out.println("Connecting with Server....");
        try {
            echoSocket = new Socket(hostName, portNumber);
            this.out = new PrintWriter(echoSocket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
            System.out.println("Connection with server complete");
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        }
    }

    // Method to send a message to the server
    public boolean sendMessage(String message) {
        int timeoutInMillis = 100;
        try {
            if (out == null) {

                serverStatus[pid] = false;
                System.err.println("Connection not established. Please connect first.");
                return false;
            }

            out.println(message);
            // Set a timeout for receiving replies
            echoSocket.setSoTimeout(timeoutInMillis); // Set the timeout in milliseconds

            String reply = in.readLine();

            if (reply == null) {
                serverStatus[pid] = false;
                System.err.println("NULL Replay: Message: '" + message + "' couldn't be delivered. Check connection");
                return false;
            }
            if (reply.equals("ACK")) {
                System.err.println("ACK: Returning True from sendMessage");
                return true;
            } else {
                System.err.println("NO ACK: Message: '" + message + "' couldn't be delivered. Reply: " + reply);
                return false;
            }

        } catch (SocketTimeoutException e) {
            // Handle timeout exception
            System.err.println("Timeout: No reply received for message: '" + message + "'");
            serverStatus[pid] = false;
            return false;
        } catch (IOException e) {
            // Handle IOException (e.g., connection failure or writing failure)
            System.err.println("Error sending message: '" + message + "'");
            // If there's an IOException, mark the server as down
            serverStatus[pid] = false;
            // Close the connection
            try {
                closeConnection();
            } catch (IOException e1) {
                // Handle the error gracefully or log it
                System.err.println("[sendMessage]: IOException " + e.getMessage());
                e1.printStackTrace();
            }
            return false;
        } catch (Exception e) {
            // Handle any other unexpected exceptions
            System.err.println("[sendMessage]: Exception " + e.getMessage());
            e.printStackTrace();

            // Add debug information for variables
            System.err.println("Debug information:");
            System.err.println("pid: " + pid);
            if (serverStatus != null) {
                System.err.println("serverStatus length: " + serverStatus.length);
            } else {
                System.err.println("serverStatus is null");
            }

            serverStatus[pid] = false; // This line might be causing the NullPointerException

            return false;
        }

    }

    // Method to close the connection
    public void closeConnection() throws IOException {
        if (echoSocket != null) {
            echoSocket.close();
        }
        if (out != null) {
            out.close();
        }
        if (in != null) {
            in.close();
        }
    }

    // Method to handle messages on a separate thread - this handles replies to the
    // messages sent to other servers. This is different from when its acting like a
    // server and receives messages from other server
    public void handleMessages() {
        Thread thread = new Thread(() -> {
            try {
                String message;
                while ((message = in.readLine()) != null) {
                    System.out.println("Handle Reply: " + message);
                    // this.clientProcess.processStringAndStore(message);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }

}

// Server Class
class MultiThreadServer extends Thread {
    final int serverPortNumber;
    final int pid;
    final Process process;

    public MultiThreadServer(int serverPortNumber, int pid, Process process) {
        this.serverPortNumber = serverPortNumber;
        this.pid = pid;
        this.process = process;
    }

    @Override
    public void run() {
        ServerSocket ss;
        try {
            ss = new ServerSocket(serverPortNumber);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server has started on " + ss.getInetAddress() + " " + ss.getLocalPort());

        // loop where server accepts new connections and each new connection is placed
        // on a new thread
        while (true) {
            Socket s = null;

            try {
                s = ss.accept();
                System.err.println("A new client is connected: " + s);

                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(s.getInputStream()));

                System.out.println("Assigning new thread for this client");

                // Determine if the connection is from a client or another server
                Thread t;
                int connectionPortNumber = s.getLocalPort();
                if (connectionPortNumber > 6000) {
                    // This is a connection from another server
                    t = new ConnectionHandler(s, in, out, pid, process);
                } else {
                    // This is a connection from a client
                    t = new ConnectionHandler(s, in, out, pid, process);
                }

                t.start();
            } catch (Exception e) {
                try {
                    if (s != null) {
                        s.close();
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    }
}

// Class to handle messages received by server by a particular client. Diffenret
// objects for different connections
class ConnectionHandler extends Thread {
    final BufferedReader in;
    final PrintWriter out;
    final Socket s;
    int pid;
    Process process;

    public ConnectionHandler(Socket s, BufferedReader in, PrintWriter out, int pid, Process process) {
        this.in = in;
        this.out = out;
        this.s = s;
        this.process = process;
        this.pid = pid;
    }

    @Override
    public void run() {
        String received;
        while (true) {
            try {
                received = in.readLine();
                System.err.println("[ConnectionHandler] Msg Rec: " + received);

                if (received.equals("Exit")) {
                    System.out.println("Client " + this.s + "sends exit.");
                    this.s.close();
                    System.err.println("Connection Closed");
                    break;
                }
                Random random = new Random();

                // This line introduces the simulation of network delay
                Thread.sleep(random.nextInt(5));
                // process the received message
                process.processMessage(out, received);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                break;
            } catch (Exception e) {
                System.err.println("[ConnectionHandler] Exception: " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }

        System.out.println(
                "Closing Server Thread. Printing Clock after processing all received messages: ");

        try {
            this.in.close();
            this.out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
