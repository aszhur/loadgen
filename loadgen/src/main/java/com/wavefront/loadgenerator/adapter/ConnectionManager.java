package com.wavefront.loadgenerator.adapter;

import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.logging.Logger;

/**
 * ConnectionManager manages a single Connection ensuring it is alive.
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class ConnectionManager implements Runnable {
    private static Logger log = Logger.getLogger(ConnectionManager.class.getName());
    private final String host;
    private final int port;
    private final int bufferSizeBytes;
    private final Random random;

    // The Connection we are giving out to clients.
    private Connection connection;

    // All Connections with an id less than firstGoodId are considered bad / stale.
    // Connections with an id greater than or equal to firstGoodId are considered good.
    private int firstGoodId;

    /**
     * @param host the WF proxy hostname
     * @param port the port
     * @param bufferSizeBytes The buffer size in bytes
     * @throws IOException
     */
    public ConnectionManager(String host, int port, int bufferSizeBytes) throws IOException {
        this.host = host;
        this.port = port;
        this.bufferSizeBytes = bufferSizeBytes;
        this.random = new Random();

        // We build the connection synchronously here giving it an ID of 1, and we consider it good
        this.connection = buildConnection(1);
        this.firstGoodId = 1;

        // Start the thread that will rebuild the connection we give out when it becomes bad or stale
        new Thread(this).start();
    }

    /**
     * Invalidate invalidates conn and returns immediately
     */
    public synchronized void invalidate(Connection conn) {

        // If the connection is already bad / stale, there is no need to do anything
        if (conn.getId() < firstGoodId) {
            return;
        }

        // Make firstGoodId exactly one more than the id of this connection so that this connection
        // will be bad / stale.
        firstGoodId = conn.getId() + 1;

        // Wake up the thread that rebuilds the connection we hand out to clients
        notify();
    }

    /**
     * getConnection returns the current connection. If a thread has invalidated what getConnection
     * returns, then this ConnectionManager will create a new Connection object in the background,
     * and getConnection will start returning the new Connection object as soon as it is created.
     */
    public synchronized Connection getConnection() {
        return connection;
    }

    public void run() {

        // This loop refreshes the connection we give out to clients.
        while (true) {
            int id = 0;
            synchronized (this) {

                // Sleep until the connection we are giving out is bad / stale
                while (firstGoodId <= connection.getId()) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // capture firstGoodId while we have the lock
                id = firstGoodId;
            }

            // Try to rebuild the connection using exponential backoff.
            // Use firstGoodId so that the new connection is considered good
            Connection conn = buildConnectionWithBackoff(id);

            // Update the connection we are handing out to clients.
            synchronized (this) {
                connection = conn;
            }
        }
    }

    private Connection buildConnectionWithBackoff(int id) {
        int timeToWaitMillis = 1000;
        while (true) {
            try {
                return buildConnection(id);
            } catch (IOException e) {
                // Do nothing
            }
            int delay = timeToWaitMillis + random.nextInt(1000);
            log.info("Reconnect failed trying again in "+delay+"ms");
            try {
                Thread.sleep(delay );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            timeToWaitMillis *= 2;
            if (timeToWaitMillis > 60000) {
                timeToWaitMillis = 60000;
            }
        }
    }

    private Connection buildConnection(int id) throws IOException {
        Socket socket = new Socket(host, port);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                socket.getOutputStream(), bufferSizeBytes);
        DataOutputStream outToServer = new DataOutputStream(bufferedOutputStream);
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        return new Connection(id, socket, outToServer, bufferedOutputStream, inFromServer);
    }

}
