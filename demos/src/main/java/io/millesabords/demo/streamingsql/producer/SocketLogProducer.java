package io.millesabords.demo.streamingsql.producer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class SocketLogProducer extends LogProducer {

    private ServerSocket serverSocket;

    private final List<Socket> clientSockets = new LinkedList<>();

    private final Random random = new Random();

    public static void main(final String[] args) {
        new SocketLogProducer().run();
    }

    public void run() {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closeConnections();
        }));

        try {
            serverSocket = new ServerSocket(5000);
            System.out.println("Waiting for connections...");

            while (true) {
                final Socket clientSocket = serverSocket.accept();
                clientSockets.add(clientSocket);

                System.out.println("New connection !");

                new Thread(() -> {
                    try {
                        final PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                        while (true) {
                            out.println(newCsvLog("\t"));
                            Thread.sleep(random.nextInt(10) + 50);
                        }
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }).start();
            }
        }
        catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    private void closeConnections() {
        System.out.println("Stopping server");
        try {
            for (final Socket clientSocket: clientSockets) {
                clientSocket.close();
            }

            if (serverSocket != null) {
                serverSocket.close();
            }
        }
        catch (final IOException e) {
        }
    }
}
