package org.dfs;

import org.dfs.node.Client;
import org.dfs.node.Controller;
import org.dfs.node.Node;
import org.dfs.node.chunkServer.ChunkServer;
import org.dfs.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Scanner;

public class InteractiveCommandParser extends Thread {
    private static final Logger log = LogManager.getLogger(InteractiveCommandParser.class);

    private final Scanner scanner;
    private final Node node;
    private final boolean acceptingCommands;

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
        try {
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
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void parseControllerCommands() {
        String nextCommand;
        Controller controller = (Controller) node;
        while (acceptingCommands) {
            nextCommand = scanner.nextLine().trim();
            if (nextCommand.contains(Constants.Controller.CMD_GET_HOST)) {
                controller.printHost();
            } else if (nextCommand.contains(Constants.Controller.CMD_LIST_CHUNK_SERVERS) ||
                    nextCommand.contains("list-cs") || nextCommand.contains("get-cs")) {
                controller.listChunkServers();
            } else if (nextCommand.equals("list-chunks") || nextCommand.contains("get-chunks")) {
                controller.printChunks(true);
            } else if (nextCommand.equals("list-chunks-summary")) {
                controller.printChunks(false);
            } else if (nextCommand.contains("list-files") || nextCommand.contains("get-files")) {
                controller.printFiles();
            } else if (nextCommand.equals("")) {
                continue;
            } else {
                System.out.println("Invalid command");
            }
        }

        log.info("Shutting down Controller");
    }

    private void parseChunkServerCommands() {
        String nextCommand;
        ChunkServer chunkServer = (ChunkServer) node;
        while (acceptingCommands) {
            nextCommand = scanner.nextLine().trim();
            if (nextCommand.contains(Constants.ChunkServer.CMD_GET_HOST)) {
                chunkServer.printHost();
            } else if (nextCommand.contains("list-chunks") || nextCommand.contains("get-chunks")) {
                chunkServer.printChunks();
            } else if (nextCommand.equals("")) {
                continue;
            } else {
                System.out.println("Invalid command");
            }
        }

        log.info("Shutting down ChunkServer");
    }

    private void parseClientCommands() throws IOException {
        String nextCommand;
        Client client = (Client) node;
        while (acceptingCommands) {
            nextCommand = scanner.nextLine().trim();
            if (nextCommand.contains(Constants.Client.CMD_ADD_FILE)) {
                // example command "add-file test.txt"
                String[] args = nextCommand.split("\\s+");
                if (args.length == 2) {
                    String filePath = args[1];
                    client.addFile(filePath);
                } else {
                    System.out.println("Invalid parameters. Please enter 'add-file <file-path>'");
                }
            } else if (nextCommand.contains("read-file") || nextCommand.contains("get-file")) {
                // example command "read-file test.txt"
                String[] args = nextCommand.split("\\s+");
                if (args.length == 2) {
                    String filePath = args[1];
                    client.retrieveFile(filePath);
                } else {
                    System.out.println("Invalid parameters. Please enter 'read-file <file-path>'");
                }
            } else if (nextCommand.contains(Constants.Client.CMD_GET_HOST)) {
                client.printHost();
            } else if (nextCommand.equals("")) {
                continue;
            } else {
                System.out.println("Invalid command");
            }
        }

        log.info("Shutting down client");
        scanner.close();
    }
}
