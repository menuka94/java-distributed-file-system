package cs555.hw1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Scanner;

public class InteractiveCommandParser extends Thread {
    private static final Logger log = LogManager.getLogger(InteractiveCommandParser.class);
    private Scanner scanner;
    private Node node;
    private boolean acceptingCommands;

    private enum Mode {
        Client, Controller, ChunkServer
    }
    private Mode mode;

    public InteractiveCommandParser(Node node) {
        this.node = node;
        scanner = new Scanner(System.in);
        acceptingCommands = true;
        if (node instanceof Controller) {
            mode = Mode.Controller;
        } else if (node instanceof Client) {
            mode = Mode.Client;
        } else if (node instanceof ChunkServer) {
            mode = Mode.ChunkServer;
        }
    }

    @Override
    public void run() {
        log.info("Starting Command Parser...");
        switch (mode) {
            case Client:
                System.out.println("Enter commands for the Client: ");
                parseClientCommands();
                break;
            case Controller:
                System.out.println("Enter commands for the Controller: ");
                parseControllerCommands();
                break;
            case ChunkServer:
                System.out.println("Enter commands for ChunkServer: ");
                parseChunkServerCommands();
                break;
            default:
                log.error("Internal Error: Unknown Node type");
        }
        parseClientCommands();
    }

    private void parseControllerCommands() {
        String nextCommand;
        Controller controller = (Controller) node;
        while (acceptingCommands) {
            nextCommand = scanner.nextLine().trim();
            if (nextCommand.contains(Constants.ChunkServer.CMD_GET_HOST)) {
                controller.printHost();
            } else if (nextCommand.trim().equals("")) {
                continue;
            } else {
                System.out.println("Invalid command");
            }
        }
    }

    private void parseChunkServerCommands() {
        String nextCommand;
        ChunkServer chunkServer = (ChunkServer) node;
        while (acceptingCommands) {
            nextCommand = scanner.nextLine().trim();
            if (nextCommand.contains(Constants.ChunkServer.CMD_GET_HOST)) {
                chunkServer.printHost();
            } else if (nextCommand.trim().equals("")) {
                continue;
            } else {
                System.out.println("Invalid command");
            }
        }
    }

    private void parseClientCommands() {
        String nextCommand;
        Client client = (Client) node;
        while (acceptingCommands) {
            nextCommand = scanner.nextLine().trim();
            if (nextCommand.contains(Constants.Client.CMD_LIST_CHUNK_SERVERS)) {
                client.listChunkServers();
            } else if (nextCommand.contains(Constants.Client.CMD_ADD_FILE)) {
                // split command to extract arguments
                // example command "add-file test.txt /home/user/Documents/test.txt"
                String[] args = nextCommand.split("\\s+");
                if (args.length == 3) {
                    String fileName = args[1];
                    String filePath = args[2];
                    client.addFile(fileName, filePath);
                } else {
                    System.out.println("Invalid parameters. Please enter 'add-file <file-name> <file-path>'");
                }

            } else if (nextCommand.trim().equals("")) {
                continue;
            } else {
                System.out.println("Invalid command");
            }
        }

        log.info("Shutting down client");
        scanner.close();
    }
}
