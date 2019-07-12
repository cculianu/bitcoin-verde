package com.softwareverde.bitcoin.server.node;

import com.softwareverde.bitcoin.server.message.type.MessageType;
import com.softwareverde.io.Logger;
import com.softwareverde.network.p2p.message.ProtocolMessage;

import java.util.HashMap;

public class MessageRouter {
    public interface MessageHandler {
        void run(ProtocolMessage message);
    }

    public interface UnknownRouteHandler {
        void run(MessageType messageType, ProtocolMessage message);
    }

    protected final HashMap<MessageType, MessageHandler> _routingTable = new HashMap<MessageType, MessageHandler>();
    protected UnknownRouteHandler _unknownRouteHandler;

    public void addRoute(final MessageType messageType, final MessageHandler messageHandler) {
        _routingTable.put(messageType, messageHandler);
    }

    public void removeRoute(final MessageType messageType) {
        _routingTable.remove(messageType);
    }

    public void route(final MessageType messageType, final ProtocolMessage message) {
        final MessageHandler messageHandler = _routingTable.get(messageType);
        if (messageHandler == null) {
            if (_unknownRouteHandler != null) { _unknownRouteHandler.run(messageType, message); }
            return;
        }

        try {
            messageHandler.run(message);
        }
        catch (final Exception exception) {
            Logger.log(exception);
        }
    }

    public void setUnknownRouteHandler(final UnknownRouteHandler unknownRouteHandler) {
        _unknownRouteHandler = unknownRouteHandler;
    }
}