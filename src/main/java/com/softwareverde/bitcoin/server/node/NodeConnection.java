package com.softwareverde.bitcoin.server.node;

import com.softwareverde.bitcoin.server.message.ProtocolMessage;
import com.softwareverde.bitcoin.server.socket.BitcoinSocket;
import com.softwareverde.io.Logger;
import com.softwareverde.util.StringUtil;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class NodeConnection {
    public interface MessageReceivedCallback {
        void onMessageReceived(ProtocolMessage message);
    }

    protected class ConnectionThread extends Thread {
        @Override
        public void run() {
            if (_socketIsConnected()) { return; }

            if (_socketUsedToBeConnected) {
                if (_onDisconnectCallback != null) {
                    (new Thread(_onDisconnectCallback)).start();
                }
                _socketUsedToBeConnected = false;
                Logger.log("IO: NodeConnection: Connection lost.");
            }

            Socket socket = null;

            while (true) {
                if (_socketIsConnected()) { break; }

                try {
                    socket = new Socket(_host, _port);
                    if (socket.isConnected()) { break; }
                    else {
                        Logger.log("IO: NodeConnection: Connection failed. Retrying in 1000ms...");
                        Thread.sleep(1000);
                    }
                }
                catch (final IOException e) { }
                catch (final InterruptedException e) { break; }
            }

            if ( (socket != null) && (socket.isConnected()) ) {
                final SocketAddress socketAddress = socket.getRemoteSocketAddress();
                {
                    final String socketIpString = socketAddress.toString();
                    final List<String> urlParts = StringUtil.pregMatch("^([^/]*)/([0-9:.]+):([0-9]+)$", socketIpString); // Example: btc.softwareverde.com/0.0.0.0:8333
                    // final String domainName = urlParts.get(0);
                    _remoteIp = urlParts.get(1); // Ip Address
                    // final String portString = urlParts.get(2);
                }

                _connection = new BitcoinSocket(socket);
                _connection.setMessageReceivedCallback(new Runnable() {
                    @Override
                    public void run() {
                        if (_messageReceivedCallback != null) {
                            _messageReceivedCallback.onMessageReceived(_connection.popMessage());
                        }
                    }
                });

                final Boolean isFirstConnection = (_connectionCount == 0);
                if (isFirstConnection) {
                    Logger.log("IO: NodeConnection: Connection established.");

                    _processOutboundMessageQueue();

                    if (_onConnectCallback != null) {
                        (new Thread(_onConnectCallback)).start();
                    }
                }
                else {
                    Logger.log("IO: NodeConnection: Connection regained.");
                    _processOutboundMessageQueue();

                    if (_onReconnectCallback != null) {
                        (new Thread(_onReconnectCallback)).start();
                    }
                }

                _socketUsedToBeConnected = true;
                _connectionCount += 1;
            }
        }
    }

    private final String _host;
    private final Integer _port;
    private String _remoteIp;

    private final Queue<ProtocolMessage> _outboundMessageQueue = new ArrayDeque<ProtocolMessage>();

    private BitcoinSocket _connection;
    private Thread _connectionThread;
    private MessageReceivedCallback _messageReceivedCallback;

    private Boolean _socketUsedToBeConnected = false;

    private Long _connectionCount = 0L;
    private Runnable _onDisconnectCallback;
    private Runnable _onReconnectCallback;
    private Runnable _onConnectCallback;

    private Boolean _socketIsConnected() {
        return ( (_connection != null) && (_connection.isConnected()) );
    }

    private void _processOutboundMessageQueue() {
        synchronized (_outboundMessageQueue) {
            while (! _outboundMessageQueue.isEmpty()) {
                if ((_connection == null) || (! _connection.isConnected())) { return; }

                final ProtocolMessage message = _outboundMessageQueue.remove();
                _connection.write(message);
            }
        }
    }

    private void _writeOrQueueMessage(final ProtocolMessage message) {
        synchronized (_outboundMessageQueue) {
            if (_socketIsConnected()) {
                _connection.write(message);
            }
            else {
                _outboundMessageQueue.add(message);
            }
        }
    }

    public NodeConnection(final String host, final Integer port) {
        _host = host;
        _port = port;

        _connectionThread = new ConnectionThread();
    }

    public void startConnectionThread() {
        _connectionThread.start();
    }

    public void stopConnectionThread() {
        _connectionThread.interrupt();
    }

    public void setOnDisconnectCallback(final Runnable callback) {
        _onDisconnectCallback = callback;
    }

    public void setOnReconnectCallback(final Runnable callback) {
        _onReconnectCallback = callback;
    }

    public void setOnConnectCallback(final Runnable callback) {
        _onConnectCallback = callback;
    }

    public Boolean isConnected() {
        return _socketIsConnected();
    }

    public void disconnect() {
        _connectionThread.interrupt();

        if (_connection != null) {
            _connection.close();
        }
    }

    public void queueMessage(final ProtocolMessage message) {
        (new Thread(new Runnable() {
            @Override
            public void run() {
                _writeOrQueueMessage(message);
            }
        })).start();
    }

    public void setMessageReceivedCallback(final MessageReceivedCallback messageReceivedCallback) {
        _messageReceivedCallback = messageReceivedCallback;
    }

    public String getRemoteIp() {
        return _remoteIp;
    }

    public String getHost() { return _host; }

    public Integer getPort() { return _port; }
}