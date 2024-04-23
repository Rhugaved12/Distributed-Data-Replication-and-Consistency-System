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

public class Server {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1 && Integer.parseInt(args[0]) >= 0 && Integer.parseInt(args[0]) < 7) {
            System.err.println("Usage: java Server <which port number to use [0-3]>");
            System.exit(1);
        }
        int[] ports = { 6001, 6002, 6003, 6004, 6005, 6006, 6007 };
        String[] hostNames = { "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
                "127.0.0.1" };
        int pid = Integer.parseInt(args[0]);
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
        // Connecting with servers
        List<EchoClient> clients = new ArrayList<>();

        for (int i = 0; i < hostNames.length; i++) {
            if (i != pid) { // Assuming 'pid' is the ID of the current server
                EchoClient client = new EchoClient(i);
                client.connect(hostNames[i], ports[i]);
                // client.handleMessages();
                clients.add(client); // Add the client to the list
            }
        }
        ClientProcess clientProcess = new ClientProcess(pid, clients);
        process.setConnectedServers(clientProcess);

        // For every server(other than self), set the clientProcess
        for (EchoClient client : clients) {
            client.setClientProcess(clientProcess);
        }
        LocalTime ctime = LocalTime.now();
        // Test: Send messages to servers
        // for (EchoClient client : clients) {

        // client.sendMessage("SERVER&" + Integer.toString(client.clientProcess.myPID) +
        // "test"
        // + Integer.toString(ports[client.clientProcess.myPID]) + " " + ctime);
        // }

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

    public synchronized void processMessage(PrintWriter out, String msg) throws IOException {
        try {
            System.err.println("[processMessage] Received MSG: " + msg + " " + this.pid);

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
                int newClockValue = objectClocks.getOrDefault(objectName, 0);
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
                out.println(storedMessages); // Send only the stored messages content

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

                // Example: Print the parsed values
                System.out.println("Client message from PID " + clientPid + ":");
                System.out.println("Clock Value: " + clockValue);
                System.out.println("Object Name: " + objectName);
                System.out.println("Message Content: " + messageContent);

                // Get stored client messages in the given object
                List<String> previouslyStoredClientMessagesList = getClientMessages(objectName);

                String lastStoredMessage = null; // Initialize to null

                if (!previouslyStoredClientMessagesList.isEmpty()) {
                    lastStoredMessage = previouslyStoredClientMessagesList
                            .get(previouslyStoredClientMessagesList.size() - 1);
                }
                int lastClockValue = objectClocks.getOrDefault(objectName, -1); // Get last clock value for the object

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
    private PrintWriter out;
    private BufferedReader in;
    public ClientProcess clientProcess;
    // this is the PID of the Server that this server is connected to.
    int pid;

    public EchoClient(int pid) {
        this.pid = pid;
    }

    public void setClientProcess(ClientProcess clientProcess) {
        this.clientProcess = clientProcess;
    }

    // Method to connect with the server
    public void connect(String hostName, int portNumber) throws IOException {
        System.out.println("Connecting with Server....");
        try {
            echoSocket = new Socket(hostName, portNumber);
            out = new PrintWriter(echoSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
            System.out.println("Connection with server complete");
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName);
            System.exit(1);
        }
    }

    // Method to send a message to the server
    public boolean sendMessage(String message) throws IOException {
        int timeoutInMillis = 100;
        if (out != null) {
            out.println(message);
            // Set a timeout for receiving replies
            echoSocket.setSoTimeout(timeoutInMillis); // Set the timeout in milliseconds
            try {
                String reply = in.readLine();
                if (reply != null && reply.equals("ACK")) {
                    return true;
                } else {
                    System.err.println("NO ACK: Message: '" + message + "' couldn't be delivered");
                    return false;
                }
            } catch (SocketTimeoutException e) {
                // Handle timeout exception
                System.err.println("Timeout: No reply received for message: '" + message + "'");
                return false;
            }
        } else {
            System.err.println("Connection not established. Please connect first.");
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
