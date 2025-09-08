package com.wavefront.loadgenerator.adapter;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * Connection contains all the artifacts for a connection to a WF proxy
 *
 * @author tk015931 (travis.keep@broadcom.com)
 */
public class Connection {
    private final int id;
    private final Socket socket;
    private final DataOutputStream outToServer;
    private final BufferedOutputStream bufferedOutputStream;
    private final BufferedReader inFromServer;

    public Connection(
            int id,
            Socket socket,
            DataOutputStream outToServer,
            BufferedOutputStream bufferedOutputStream,
            BufferedReader inFromServer) {
        this.id = id;
        this.socket = socket;
        this.outToServer = outToServer;
        this.bufferedOutputStream = bufferedOutputStream;
        this.inFromServer = inFromServer;
    }

    /**
     * Id is how the ConnectionManager tells if a Connection is stale or not.
     */
    public int getId() { return id; }

    /**
     * getSocket returns the socket to the WF proxy
     */
    public Socket getSocket() { return socket; }

    /**
     * getOutToServer returns the output stream of the socket wrapped in a DataOutputStream
     */
    public DataOutputStream getOutToServer() { return outToServer; }

    /**
     * getBufferedOutputStream returns the output stream of the socket wrapped in a BufferedOutputStream
     */
    public BufferedOutputStream getBufferedOutputStream() { return bufferedOutputStream; }

    /**
     * getInFromServer returns the input stream of the socket wrapped in a BufferedReader.
     */
    public BufferedReader getInFromServer() { return inFromServer; }
}
