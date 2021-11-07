package com.softwareverde.bitcoin.server.module.stratum;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderDeflater;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.merkleroot.MerkleTree;
import com.softwareverde.bitcoin.block.merkleroot.MerkleTreeNode;
import com.softwareverde.bitcoin.block.merkleroot.MutableMerkleTree;
import com.softwareverde.bitcoin.merkleroot.MerkleRoot;
import com.softwareverde.bitcoin.rpc.NodeJsonRpcConnection;
import com.softwareverde.bitcoin.server.configuration.ElectrumProperties;
import com.softwareverde.bitcoin.server.electrum.socket.ElectrumServerSocket;
import com.softwareverde.bitcoin.server.message.type.query.header.RequestBlockHeadersMessage;
import com.softwareverde.bitcoin.server.module.node.sync.bootstrap.HeadersBootstrapper;
import com.softwareverde.bitcoin.server.module.stratum.json.ElectrumJson;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.script.ScriptBuilder;
import com.softwareverde.bitcoin.transaction.script.ScriptPatternMatcher;
import com.softwareverde.bitcoin.transaction.script.ScriptType;
import com.softwareverde.bitcoin.transaction.script.locking.LockingScript;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.http.tls.TlsCertificate;
import com.softwareverde.http.tls.TlsFactory;
import com.softwareverde.json.Json;
import com.softwareverde.json.Jsonable;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.socket.JsonProtocolMessage;
import com.softwareverde.network.socket.JsonSocket;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Util;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ElectrumModule {

    protected static class AddressSubscriptionKey {
        public final Sha256Hash scriptHash;
        public final String subscriptionString;
        public final Boolean isScriptHash;

        public AddressSubscriptionKey(final Address address, final String subscriptionString) {
            this.scriptHash = ScriptBuilder.computeScriptHash(address);
            this.subscriptionString = subscriptionString;
            this.isScriptHash = false;
        }

        public AddressSubscriptionKey(final Sha256Hash scriptHash, final String subscriptionString) {
            this.scriptHash = scriptHash;
            this.subscriptionString = subscriptionString;
            this.isScriptHash = true;
        }

        @Override
        public boolean equals(final Object object) {
            if (! (object instanceof AddressSubscriptionKey)) { return false; }
            final AddressSubscriptionKey addressSubscriptionKey = (AddressSubscriptionKey) object;
            return Util.areEqual(this.scriptHash, addressSubscriptionKey.scriptHash);
        }

        @Override
        public int hashCode() {
            return this.scriptHash.hashCode();
        }

        @Override
        public String toString() {
            return this.scriptHash + ":" + (this.isScriptHash ? "Hash" : "Address");
        }
    }

    protected static class ConnectionAddress {
        public final AddressSubscriptionKey subscriptionKey;
        public final WeakReference<JsonSocket> connection;
        public Sha256Hash status;

        public ConnectionAddress(final AddressSubscriptionKey subscriptionKey, final JsonSocket jsonSocket) {
            this.subscriptionKey = subscriptionKey;
            this.connection = new WeakReference<>(jsonSocket);
        }

        @Override
        public String toString() {
            return this.subscriptionKey + ":" + this.status;
        }
    }

    protected static class TransactionPosition implements Comparable<TransactionPosition>, Jsonable {
        public static final Comparator<TransactionPosition> COMPARATOR = new Comparator<TransactionPosition>() {
            @Override
            public int compare(final TransactionPosition transactionPosition0, final TransactionPosition transactionPosition1) {
                if (transactionPosition0.isUnconfirmedTransaction()) {
                    return (transactionPosition1.isUnconfirmedTransaction() ? 0 : 1);
                }
                else if (transactionPosition1.isUnconfirmedTransaction()) {
                    return -1;
                }

                final int blockHeightCompare = transactionPosition0.blockHeight.compareTo(transactionPosition1.blockHeight);
                if (blockHeightCompare != 0) { return blockHeightCompare; }
                return transactionPosition0.transactionIndex.compareTo(transactionPosition1.transactionIndex);
            }
        };

        public final Long blockHeight;
        public final Integer transactionIndex;
        public final Sha256Hash transactionHash;
        public final Boolean hasUnconfirmedInputs;
        public Long transactionFee;

        protected Long _getBlockHeight() {
            if (this.isUnconfirmedTransaction()) {
                return (this.hasUnconfirmedInputs ? -1L : 0L);
            }

            return this.blockHeight;
        }

        public TransactionPosition(final Long blockHeight, final Integer transactionIndex, final Boolean hasUnconfirmedInputs, final Sha256Hash transactionHash) {
            this.blockHeight = blockHeight;
            this.transactionIndex = transactionIndex;
            this.transactionHash = transactionHash;
            this.hasUnconfirmedInputs = hasUnconfirmedInputs;
        }

        public Boolean isUnconfirmedTransaction() {
            return (this.blockHeight == null);
        }

        @Override
        public boolean equals(final Object object) {
            if (! (object instanceof TransactionPosition)) { return false; }
            final TransactionPosition transactionPosition = (TransactionPosition) object;

            if (! Util.areEqual(this.blockHeight, transactionPosition.blockHeight)) { return false; }
            if (! Util.areEqual(this.transactionIndex, transactionPosition.transactionIndex)) { return false; }
            return true;
        }

        @Override
        public int compareTo(final TransactionPosition transactionPosition) {
            return TransactionPosition.COMPARATOR.compare(this, transactionPosition);
        }

        @Override
        public int hashCode() {
            final Long blockHeight = _getBlockHeight();
            return Long.hashCode(blockHeight);
        }

        @Override
        public String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            final String transactionHashString = this.transactionHash.toString();
            final String transactionHashLowerCaseString = transactionHashString.toLowerCase();
            final Long blockHeight = _getBlockHeight();

            stringBuilder.append(transactionHashLowerCaseString);
            stringBuilder.append(":");
            stringBuilder.append(blockHeight);
            stringBuilder.append(":");
            return stringBuilder.toString();
        }

        @Override
        public Json toJson() {
            final Long blockHeight = _getBlockHeight();
            final Long transactionFee = this.transactionFee;

            final Json json = new ElectrumJson(false);
            json.put("height", blockHeight);
            json.put("tx_hash", this.transactionHash);
            if (transactionFee != null) {
                json.put("fee", transactionFee);
            }
            return json;
        }
    }

    protected final Long _minTransactionFeePerByte;

    protected final ElectrumProperties _electrumProperties;
    protected final Boolean _tlsIsEnabled;
    protected final CachedThreadPool _threadPool;
    protected final ElectrumServerSocket _electrumServerSocket;
    protected final ConcurrentHashMap<Long, JsonSocket> _connections = new ConcurrentHashMap<>();
    protected final HashMap<AddressSubscriptionKey, LinkedList<ConnectionAddress>> _connectionAddresses = new HashMap<>();

    protected final MutableList<BlockHeader> _cachedBlockHeaders = new MutableList<>(0);
    protected Long _chainHeight = 0L;

    protected final Thread _maintenanceThread;
    protected NodeJsonRpcConnection _nodeNotificationConnection;

    protected NodeJsonRpcConnection _getNodeConnection() {
        final String nodeHost = _electrumProperties.getBitcoinRpcUrl();
        final Integer nodePort = _electrumProperties.getBitcoinRpcPort();
        return new NodeJsonRpcConnection(nodeHost, nodePort, _threadPool);
    }

    protected void _createNodeNotificationConnection() {
        if (_nodeNotificationConnection != null) {
            _nodeNotificationConnection.close();
            _nodeNotificationConnection = null;
        }

        final NodeJsonRpcConnection nodeJsonRpcConnection = _getNodeConnection();
        _nodeNotificationConnection = nodeJsonRpcConnection;

        nodeJsonRpcConnection.upgradeToAnnouncementHook(new NodeJsonRpcConnection.RawAnnouncementHookCallback() {
            @Override
            public void onNewBlockHeader(final BlockHeader blockHeader) {
                _onNewHeader(blockHeader);
            }

            @Override
            public void onNewTransaction(final Transaction transaction, final Long fee) {
                _onNewTransaction(transaction, fee);
            }
        });
    }

    protected void _onNewHeader(final BlockHeader blockHeader) {
        final Sha256Hash blockHash = blockHeader.getHash();

        final Long blockHeight;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            final Json blockHeightJson = nodeConnection.getBlockHeaderHeight(blockHash);
            blockHeight = blockHeightJson.getLong("blockHeight");
        }

        Logger.debug("New Header: " + blockHash + " " + blockHeight);

        for (final JsonSocket socket : _connections.values()) {
            _notifyBlockHeader(socket, blockHeader, blockHeight);
        }

        synchronized (_connectionAddresses) {
            for (final Map.Entry<AddressSubscriptionKey, LinkedList<ConnectionAddress>> entry : _connectionAddresses.entrySet()) {
                for (final ConnectionAddress connectionAddress : entry.getValue()) {
                    final JsonSocket jsonSocket = connectionAddress.connection.get();
                    final boolean isConnected = ((jsonSocket != null) && jsonSocket.isConnected());
                    if (! isConnected) { continue; }

                    final Sha256Hash addressStatus = _calculateAddressStatus(connectionAddress.subscriptionKey);
                    if (! Util.areEqual(addressStatus, connectionAddress.status)) {
                        connectionAddress.status = addressStatus;
                        Logger.debug("Updated Status: " + connectionAddress);

                        if (connectionAddress.subscriptionKey.isScriptHash) {
                            _notifyScriptHashStatus(jsonSocket, connectionAddress.subscriptionKey, addressStatus);
                        }
                        else {
                            _notifyAddressStatus(jsonSocket, connectionAddress.subscriptionKey, addressStatus);
                        }
                    }
                }
            }
        }
    }

    protected void _onNewTransaction(final Transaction transaction, final Long fee) {
        final ScriptPatternMatcher scriptPatternMatcher = new ScriptPatternMatcher();

        final HashSet<Address> matchedAddresses = new HashSet<>();
        for (final TransactionOutput transactionOutput : transaction.getTransactionOutputs()) {
            final LockingScript lockingScript = transactionOutput.getLockingScript();

            final ScriptType scriptType = scriptPatternMatcher.getScriptType(lockingScript);
            if (scriptType == null) { continue; }

            final Address address = scriptPatternMatcher.extractAddress(scriptType, lockingScript);
            if (address == null) { continue; }

            final AddressSubscriptionKey addressSubscriptionKey = new AddressSubscriptionKey(address, null);
            synchronized (_connectionAddresses) {
                final boolean addressIsConnected = _connectionAddresses.containsKey(addressSubscriptionKey);
                if (addressIsConnected) {
                    matchedAddresses.add(address);
                }
            }
        }

        for (final Address address : matchedAddresses) {
            synchronized (_connectionAddresses) {
                final LinkedList<ConnectionAddress> connectionAddresses = _connectionAddresses.get(new AddressSubscriptionKey(address, null));
                for (final ConnectionAddress connectionAddress : connectionAddresses) {
                    final JsonSocket jsonSocket = connectionAddress.connection.get();
                    final boolean jsonSocketIsConnected = ((jsonSocket != null) && jsonSocket.isConnected());
                    if (!jsonSocketIsConnected) { continue; }

                    if (connectionAddress.subscriptionKey.isScriptHash) {
                        _notifyScriptHashStatus(jsonSocket, connectionAddress.subscriptionKey);
                    }
                    else {
                        final Sha256Hash status = _calculateAddressStatus(connectionAddress.subscriptionKey);
                        connectionAddress.status = status;
                        Logger.debug("Updated Status: " + connectionAddress);

                        _notifyAddressStatus(jsonSocket, connectionAddress.subscriptionKey, status);
                    }
                }
            }
        }
    }

    protected Long _loadBootstrappedHeaders() {
        long blockHeight = -1L;

        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
        try (final InputStream inputStream = HeadersBootstrapper.class.getResourceAsStream("/bootstrap/headers.dat")) {
            if (inputStream == null) {
                Logger.warn("Unable to open headers bootstrap file.");
                return blockHeight;
            }

            final MutableByteArray buffer = new MutableByteArray(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT);
            while (true) {
                int readByteCount = inputStream.read(buffer.unwrap());
                while ((readByteCount >= 0) && (readByteCount < BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT)) {
                    final int nextByte = inputStream.read();
                    if (nextByte < 0) { break; }

                    buffer.setByte(readByteCount, (byte) nextByte);
                    readByteCount += 1;
                }
                if (readByteCount != BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT) { return blockHeight; }

                final BlockHeader blockHeader = blockHeaderInflater.fromBytes(buffer);
                if (blockHeader == null) { return blockHeight; }

                _cachedBlockHeaders.add(blockHeader);
                blockHeight += 1L;
            }
        }
        catch (final Exception exception) {
            Logger.warn(exception);
        }

        return blockHeight;
    }

    protected void _cacheBlockHeaders() {
        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            final Json blockHeightJson = nodeConnection.getBlockHeight();
            _chainHeight = blockHeightJson.getLong("blockHeight");
            _cachedBlockHeaders.clear();

            final long maxBlockHeight = Math.max(0L, _chainHeight - RequestBlockHeadersMessage.MAX_BLOCK_HEADER_HASH_COUNT);
            long blockHeight = (_loadBootstrappedHeaders() + 1L);
            while (blockHeight <= maxBlockHeight) {
                final Json blockHeadersJson = nodeConnection.getBlockHeadersAfter(blockHeight, RequestBlockHeadersMessage.MAX_BLOCK_HEADER_HASH_COUNT, true);
                final Json blockHeadersArray = blockHeadersJson.get("blockHeaders");
                final int blockHeaderCount = blockHeadersArray.length();
                Logger.debug("Received " + blockHeaderCount + " headers, starting at: " + blockHeight + " max=" + maxBlockHeight);
                for (int i = 0; i < blockHeaderCount; ++i) {
                    final String blockHeaderString = blockHeadersArray.getString(i);
                    final BlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString(blockHeaderString));
                    _cachedBlockHeaders.add(blockHeader);
                    blockHeight += 1L;

                    if (blockHeight > maxBlockHeight) { break; } // Include the maxBlockHeight block...
                }
            }
        }
    }

    protected MerkleTree<BlockHeader> _calculateCheckpointMerkle(final Long maxBlockHeight) {
        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();

        final MutableMerkleTree<BlockHeader> blockHeaderMerkleTree = new MerkleTreeNode<>();
        long blockHeight;
        {
            blockHeight = 0L;
            while (blockHeight < maxBlockHeight) {
                if (blockHeight >= _cachedBlockHeaders.getCount()) { break; }

                final BlockHeader blockHeader = _cachedBlockHeaders.get((int) blockHeight);
                blockHeaderMerkleTree.addItem(blockHeader);
                blockHeight += 1L;
            }
        }

        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            while (blockHeight <= maxBlockHeight) {
                final Json blockHeadersJson = nodeConnection.getBlockHeadersAfter(blockHeight, RequestBlockHeadersMessage.MAX_BLOCK_HEADER_HASH_COUNT, true);
                final Json blockHeadersArray = blockHeadersJson.get("blockHeaders");
                final int blockHeaderCount = blockHeadersArray.length();
                Logger.debug("Received " + blockHeaderCount + " headers, starting at: " + blockHeight + " max=" + maxBlockHeight);
                for (int i = 0; i < blockHeaderCount; ++i) {
                    final String blockHeaderString = blockHeadersArray.getString(i);
                    final BlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString(blockHeaderString));
                    blockHeaderMerkleTree.addItem(blockHeader);
                    blockHeight += 1L;

                    if (blockHeight > maxBlockHeight) { break; } // Include the maxBlockHeight block...
                }
            }
        }

        Logger.trace("Calculated: " + blockHeaderMerkleTree.getMerkleRoot());
        return blockHeaderMerkleTree;
    }

    protected TlsCertificate _loadCertificate(final String certificateFile, final String certificateKeyFile) {
        if (Util.isBlank(certificateFile) || Util.isBlank(certificateKeyFile)) { return null; }

        final TlsFactory tlsFactory = new TlsFactory();

        final byte[] certificateBytes = IoUtil.getFileContents(certificateFile);
        final byte[] certificateKeyFileBytes = IoUtil.getFileContents(certificateKeyFile);
        if ( (certificateBytes == null) || (certificateKeyFileBytes == null) ) {
            Logger.error("Error loading certificate: " + certificateFile + ", " + certificateKeyFile);
            return null;
        }

        tlsFactory.addTlsCertificate(StringUtil.bytesToString(certificateBytes), certificateKeyFileBytes);
        return tlsFactory.buildCertificate();
    }

    protected void _handleServerVersionMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Json json = new ElectrumJson(false);
        final Json resultJson = new ElectrumJson(true);
        resultJson.add("ElectrumVerde 1.0.0");
        resultJson.add("1.4");

        json.put("id", id);
        json.put("result", resultJson);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleBannerMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Json json = new ElectrumJson(false);

        json.put("id", id);
        json.put("result", "ElectrumVerde 1.0.0");
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleDonationAddressMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Json json = new ElectrumJson(false);

        json.put("id", id);
        json.put("result", "qqverdefl9xtryyx8y52m6va5j8s2s4eq59fjdn97e");
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handlePingMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Json json = new ElectrumJson(false);

        json.put("id", id);
        json.put("result", null);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleMinimumRelayFeeMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Json json = new ElectrumJson(false);

        final double minRelayFee = ((_minTransactionFeePerByte * 1000L) / Transaction.SATOSHIS_PER_BITCOIN.doubleValue());

        json.put("id", id);
        json.put("result", minRelayFee); // Float; in Bitcoins, not Satoshis...
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleEstimateFeeMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Long blockCount;
        {
            final Json parameters = message.get("params");
            blockCount = parameters.getLong(0);
        }

        final double minRelayFee = ((_minTransactionFeePerByte * 1000L) / Transaction.SATOSHIS_PER_BITCOIN.doubleValue());

        final Json json = new ElectrumJson(false);
        json.put("id", id);
        json.put("result", minRelayFee); // Float; in Bitcoins, not Satoshis...
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _notifyBlockHeader(final JsonSocket jsonSocket, final BlockHeader blockHeader, final Long blockHeight) {
        final BlockHeaderDeflater blockHeaderDeflater = new BlockHeaderDeflater();
        final ByteArray blockHeaderBytes = blockHeaderDeflater.toBytes(blockHeader);

        final Json blockHeaderJson = new ElectrumJson(false);
        blockHeaderJson.put("height", blockHeight);
        blockHeaderJson.put("hex", blockHeaderBytes);

        final Json paramsJson = new ElectrumJson(true);
        paramsJson.add(blockHeaderJson);

        final Json json = new ElectrumJson(false);
        json.put("method", "blockchain.headers.subscribe");
        json.put("params", paramsJson);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleSubscribeBlockHeadersMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final ByteArray blockHeaderBytes;
        final Long blockHeight;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();

            final Json blockHeadersJson = nodeConnection.getBlockHeadersBeforeHead(1, true);
            final Json blockHeadersArray = blockHeadersJson.get("blockHeaders");
            final String headerString = blockHeadersArray.getString(0);
            blockHeaderBytes = ByteArray.fromHexString(headerString);
            final BlockHeader blockHeader = blockHeaderInflater.fromBytes(blockHeaderBytes);

            final Json blockHeaderHeightJson = nodeConnection.getBlockHeaderHeight(blockHeader.getHash());
            blockHeight = blockHeaderHeightJson.getLong("blockHeight");
        }

        final Json blockHeaderJson = new ElectrumJson(false);
        blockHeaderJson.put("height", blockHeight);
        blockHeaderJson.put("hex", blockHeaderBytes);

        final Json json = new ElectrumJson(false);
        json.put("id", id);
        json.put("result", blockHeaderJson);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected static class GetBlockHeadersResult {
        public final int blockHeaderCount;
        public final String blockHeadersHex;
        public final MerkleRoot blockHeadersMerkleRoot;
        public final List<Sha256Hash> blockHeadersPartialMerkleTree;

        public GetBlockHeadersResult(final int blockHeaderCount, final String blockHeadersHex, final MerkleRoot blockHeadersMerkleRoot, final List<Sha256Hash> blockHeadersPartialMerkleTree) {
            this.blockHeaderCount = blockHeaderCount;
            this.blockHeadersHex = blockHeadersHex;
            this.blockHeadersMerkleRoot = blockHeadersMerkleRoot;
            this.blockHeadersPartialMerkleTree = blockHeadersPartialMerkleTree;
        }
    }

    protected GetBlockHeadersResult _getBlockHeaders(final Long requestedBlockHeight, final Integer requestedBlockCount, final Long checkpointBlockHeight) {
        final int blockHeaderCount;
        final String concatenatedHeadersHexString;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            final Json blockHeadersJson = nodeConnection.getBlockHeadersAfter(requestedBlockHeight, requestedBlockCount, true);

            final Json blockHeadersArray = blockHeadersJson.get("blockHeaders");
            blockHeaderCount = blockHeadersArray.length();
            final StringBuilder headerStringBuilder = new StringBuilder();
            for (int i = 0; i < blockHeaderCount; ++i) {
                final String blockHeaderHexString = blockHeadersArray.getString(i);
                headerStringBuilder.append(blockHeaderHexString.toLowerCase());
            }

            concatenatedHeadersHexString = headerStringBuilder.toString();
        }

        final MerkleRoot merkleRoot;
        final List<Sha256Hash> partialMerkleTree;
        if (checkpointBlockHeight > 0L) {
            final MerkleTree<BlockHeader> blockHeaderMerkleTree = _calculateCheckpointMerkle(checkpointBlockHeight);
            final int headerIndex = (blockHeaderMerkleTree.getItemCount() - 1);

            merkleRoot = blockHeaderMerkleTree.getMerkleRoot();
            partialMerkleTree = blockHeaderMerkleTree.getPartialTree(headerIndex);

        }
        else {
            merkleRoot = null;
            partialMerkleTree = null;
        }

        return new GetBlockHeadersResult(blockHeaderCount, concatenatedHeadersHexString, merkleRoot, partialMerkleTree);
    }

    protected void _handleSubmitTransactionMessage(final JsonSocket jsonSocket, final Json message) {
        final TransactionInflater transactionInflater = new TransactionInflater();
        final Integer id = message.getInteger("id");

        final Transaction transaction;
        {
            final Json parameters = message.get("params");
            final String transactionHex = parameters.getString(0);
            transaction = transactionInflater.fromBytes(ByteArray.fromHexString(transactionHex));

            if (transaction == null) {
                Logger.debug("Invalid Transaction hex.");
                return;
            }
        }

        final Sha256Hash transactionHash;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.submitTransaction(transaction);
            transactionHash = transaction.getHash();
        }

        final Json json = new ElectrumJson(false);
        json.put("id", id);
        json.put("result", transactionHash);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleBlockHeadersMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Long requestedBlockHeight;
        final int requestedBlockCount;
        final Long checkpointBlockHeight;
        {
            final Json parameters = message.get("params");
            requestedBlockHeight = parameters.getLong(0);
            requestedBlockCount = Math.min(RequestBlockHeadersMessage.MAX_BLOCK_HEADER_HASH_COUNT, parameters.getInteger(1));
            checkpointBlockHeight = parameters.getLong(2);
        }

        final GetBlockHeadersResult blockHeadersResult = _getBlockHeaders(requestedBlockHeight, requestedBlockCount, checkpointBlockHeight);

        final Json resultJson = new ElectrumJson(false);
        resultJson.put("count", blockHeadersResult.blockHeaderCount);
        resultJson.put("hex", blockHeadersResult.blockHeadersHex); // Confirmed correct endianness.
        resultJson.put("max", RequestBlockHeadersMessage.MAX_BLOCK_HEADER_HASH_COUNT);

        if (checkpointBlockHeight > 0L) {
            resultJson.put("root", blockHeadersResult.blockHeadersMerkleRoot); // Confirmed correct and correct endianness.

            final Json merkleHashesJson = new ElectrumJson(true);
            for (final Sha256Hash merkleHash : blockHeadersResult.blockHeadersPartialMerkleTree) {
                merkleHashesJson.add(merkleHash); // Confirmed correct and correct endianness.
            }
            resultJson.put("branch", merkleHashesJson);
        }

        final Json json = new ElectrumJson(false);
        json.put("id", id);
        json.put("result", resultJson);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleBlockHeaderMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Long requestedBlockHeight;
        final Long checkpointBlockHeight;
        {
            final Json parameters = message.get("params");
            requestedBlockHeight = parameters.getLong(0);
            checkpointBlockHeight = parameters.getLong(1);
        }

        final GetBlockHeadersResult blockHeadersResult = _getBlockHeaders(requestedBlockHeight, 1, checkpointBlockHeight);

        final Json json = new ElectrumJson(false);
        json.put("id", id);

        if (checkpointBlockHeight > 0L) {
            final Json resultJson = new ElectrumJson(false);
            resultJson.put("header", blockHeadersResult.blockHeadersHex);
            resultJson.put("root", blockHeadersResult.blockHeadersMerkleRoot);

            final Json merkleHashesJson = new ElectrumJson(true);
            for (final Sha256Hash merkleHash : blockHeadersResult.blockHeadersPartialMerkleTree) {
                merkleHashesJson.add(merkleHash);
            }
            resultJson.put("branch", merkleHashesJson);
            json.put("result", resultJson);
        }
        else {
            json.put("result", blockHeadersResult.blockHeadersHex); // Confirmed correct endian.
        }

        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleGetTransactionMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Boolean verboseFormat;
        final Sha256Hash transactionHash;
        {
            final Json parameters = message.get("params");
            final String transactionHashString = parameters.getString(0);
            transactionHash = Sha256Hash.fromHexString(transactionHashString);
            if (transactionHash == null) {
                Logger.debug("Invalid Transaction Hash: " + transactionHashString);
                return;
            }

            verboseFormat = parameters.getBoolean(1);
            if (verboseFormat) {
                Logger.debug("Unsupported Get-Transaction mode.");
                return;
            }
        }

        final Json json = new ElectrumJson(false);
        json.put("id", id);

        final String transactionHexString;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            final Json transactionJson = nodeConnection.getTransaction(transactionHash, true);
            final String transactionHexUppercaseString = transactionJson.getString("transaction");
            transactionHexString = transactionHexUppercaseString.toLowerCase();
        }

        json.put("result", transactionHexString);


        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleGetTransactionMerkleProofMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Long blockHeight;
        final Sha256Hash transactionHash;
        {
            final Json parameters = message.get("params");
            final String transactionHashString = parameters.getString(0);
            transactionHash = Sha256Hash.fromHexString(transactionHashString);
            if (transactionHash == null) {
                Logger.debug("Invalid Transaction Hash: " + transactionHashString);
                return;
            }

            blockHeight = parameters.getLong(1);
        }

        final Json json = new ElectrumJson(false);
        json.put("id", id);

        final Json resultJson = new ElectrumJson(false);
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            final Json transactionBlockHeightJson = nodeConnection.getTransactionBlockHeight(transactionHash);
            final Long actualBlockHeight = transactionBlockHeightJson.getOrNull("blockHeight", Json.Types.LONG);
            final Integer transactionIndex = transactionBlockHeightJson.getOrNull("transactionIndex", Json.Types.INTEGER);
            final Boolean hasUnconfirmedInputs = transactionBlockHeightJson.getBoolean("hasUnconfirmedInputs");

            if ( (actualBlockHeight == null) || (! Util.areEqual(actualBlockHeight, blockHeight))) {
                Logger.debug("Transaction not found: " + transactionHash + ":" + blockHeight);
                return;
            }

            final BlockInflater blockInflater = new BlockInflater();
            final Json blockJson = nodeConnection.getBlock(blockHeight, true);
            final Block block = blockInflater.fromBytes(ByteArray.fromHexString(blockJson.getString("block")));

            final List<Sha256Hash> partialMerkleTree = block.getPartialMerkleTree(transactionIndex);
            final Json partialMerkleTreeJson = new ElectrumJson(true);
            for (final Sha256Hash item : partialMerkleTree) {
                partialMerkleTreeJson.add(item);
            }

            resultJson.put("block_height", actualBlockHeight);
            resultJson.put("merkle", partialMerkleTreeJson);
            resultJson.put("pos", transactionIndex);
        }

        json.put("result", resultJson);


        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleConvertAddressToScriptHashMessage(final JsonSocket jsonSocket, final Json message) {
        final AddressInflater addressInflater = new AddressInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final Address address;
        {
            final String addressString = paramsJson.getString(0);
            address = Util.coalesce(addressInflater.fromBase32Check(addressString), addressInflater.fromBase58Check(addressString));
            if (address == null) { return; }
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(address, null);

        final Json json = new ElectrumJson(false);
        json.put("id", id);
        json.put("result", addressKey.scriptHash);
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _handleGetTransactionFromBlockPositionMessage(final JsonSocket jsonSocket, final Json message) {
        final BlockInflater blockInflater = new BlockInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final Sha256Hash blockHash;
        final Integer transactionIndex;
        final Boolean includePartialMerkleTree;
        {
            blockHash = Sha256Hash.fromHexString(paramsJson.getString(0));
            transactionIndex = paramsJson.getInteger(1);
            includePartialMerkleTree = paramsJson.getBoolean(2);

            if ( (blockHash == null) || (transactionIndex < 0) ) { return; }
        }

        final Sha256Hash transactionHash;
        final List<Sha256Hash> partialMerkleTree;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            final Json blockJson = nodeConnection.getBlock(blockHash, true);
            final Block block = blockInflater.fromBytes(ByteArray.fromHexString(blockJson.getString("block")));

            final List<Transaction> transactions = block.getTransactions();
            if (transactionIndex >= transactions.getCount()) { return; }

            final Transaction transaction = transactions.get(transactionIndex);

            transactionHash = transaction.getHash();
            partialMerkleTree = (includePartialMerkleTree ? block.getPartialMerkleTree(transactionIndex) : null);
        }

        final Json json = new ElectrumJson(false);
        json.put("id", id);


        if (includePartialMerkleTree) {
            final Json resultJson = new Json();
            resultJson.put("tx_hash", transactionHash);

            final Json merkleTreeJson = new ElectrumJson(true);
            for (final Sha256Hash itemHash : partialMerkleTree) {
                merkleTreeJson.add(itemHash);
            }
            resultJson.put("merkle", merkleTreeJson);

            json.put("result", resultJson);
        }
        else {
            json.put("result", transactionHash);
        }

        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected Sha256Hash _getCachedAddressStatus(final AddressSubscriptionKey addressKey) {
        synchronized (_connectionAddresses) {
            final LinkedList<ConnectionAddress> connectionAddresses = _connectionAddresses.get(addressKey);
            if (connectionAddresses == null) { return null; }

            for (final ConnectionAddress connectionAddress : connectionAddresses) {
                final Sha256Hash status = connectionAddress.status;
                if (status == null) { continue; }

                return status;
            }

            return null;
        }
    }

    protected Sha256Hash _calculateAddressStatus(final AddressSubscriptionKey addressKey) {
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            final MutableList<TransactionPosition> transactionPositions;
            {
                final TransactionInflater transactionInflater = new TransactionInflater();
                final Json addressTransaction = nodeConnection.getAddressTransactions(addressKey.scriptHash, true);
                final Json transactionsArray = addressTransaction.get("transactions");
                final Integer transactionCount = transactionsArray.length();
                if (transactionCount < 1) { return null; }

                transactionPositions = new MutableList<>(transactionCount);
                for (int i = 0; i < transactionCount; ++i) {
                    final Sha256Hash transactionHash;
                    final Transaction transaction;
                    {
                        final String transactionHex = transactionsArray.getString(i);
                        transaction = transactionInflater.fromBytes(ByteArray.fromHexString((transactionHex)));
                        transactionHash = transaction.getHash();
                    }
                    Logger.debug(transactionHash);

                    final Json transactionBlockHeightJson = nodeConnection.getTransactionBlockHeight(transactionHash);
                    final Long blockHeight = transactionBlockHeightJson.getOrNull("blockHeight", Json.Types.LONG);
                    final Integer transactionIndex = transactionBlockHeightJson.getOrNull("transactionIndex", Json.Types.INTEGER);
                    final Boolean hasUnconfirmedInputs = transactionBlockHeightJson.getBoolean("hasUnconfirmedInputs");

                    Logger.debug(blockHeight);
                    Logger.debug(transactionIndex);
                    Logger.debug(hasUnconfirmedInputs);
                    Logger.debug(transactionBlockHeightJson);

                    final TransactionPosition transactionPosition = new TransactionPosition(blockHeight, transactionIndex, hasUnconfirmedInputs, transactionHash);
                    transactionPositions.add(transactionPosition);
                }
            }
            transactionPositions.sort(TransactionPosition.COMPARATOR);
            Logger.debug(transactionPositions.getCount());

            try {
                final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                for (final TransactionPosition transactionPosition : transactionPositions) {
                    final String statusString = transactionPosition.toString();
                    Logger.trace(statusString);
                    messageDigest.update(StringUtil.stringToBytes(statusString));
                }
                return Sha256Hash.wrap(messageDigest.digest());
            }
            catch (final Exception exception) {
                Logger.debug(exception);
                return null;
            }
        }
    }

    protected void _notifyScriptHashStatus(final JsonSocket jsonSocket, final AddressSubscriptionKey addressKey) {
        final Sha256Hash addressStatus = _calculateAddressStatus(addressKey);
        _notifyScriptHashStatus(jsonSocket, addressKey, addressStatus);
    }

    protected void _notifyScriptHashStatus(final JsonSocket jsonSocket, final AddressSubscriptionKey addressKey, final Sha256Hash addressStatus) {
        final String addressString = addressKey.subscriptionString;

        final Json responseJson = new ElectrumJson(true);
        responseJson.add(addressString);
        responseJson.add(addressStatus);

        final Json notificationJson = new ElectrumJson(false);
        notificationJson.put("method", "blockchain.scripthash.subscribe");
        notificationJson.put("params", responseJson);
        jsonSocket.write(new JsonProtocolMessage(notificationJson));
        Logger.debug("Wrote: " + notificationJson);
        jsonSocket.flush();
    }

    protected void _notifyAddressStatus(final JsonSocket jsonSocket, final AddressSubscriptionKey addressKey) {
        final Sha256Hash addressStatus = _calculateAddressStatus(addressKey);
        _notifyAddressStatus(jsonSocket, addressKey, addressStatus);
    }

    protected void _notifyAddressStatus(final JsonSocket jsonSocket, final AddressSubscriptionKey addressKey, final Sha256Hash addressStatus) {
        final String addressString = addressKey.subscriptionString;

        final Json responseJson = new ElectrumJson(true);
        responseJson.add(addressString);
        responseJson.add(addressStatus);

        final Json notificationJson = new ElectrumJson(false);
        notificationJson.put("method", "blockchain.address.subscribe");
        notificationJson.put("params", responseJson);
        jsonSocket.write(new JsonProtocolMessage(notificationJson));
        Logger.debug("Wrote: " + notificationJson);
        jsonSocket.flush();
    }

    protected void _addAddressSubscription(final AddressSubscriptionKey addressKey, final JsonSocket jsonSocket, final Sha256Hash addressStatus) {
        synchronized (_connectionAddresses) {
            final LinkedList<ConnectionAddress> connectionAddresses;
            if (_connectionAddresses.containsKey(addressKey)) {
                connectionAddresses = _connectionAddresses.get(addressKey);
            }
            else {
                connectionAddresses = new LinkedList<>();
                _connectionAddresses.put(addressKey, connectionAddresses);
            }

            { // Add the JsonSocket as a WeakReference if the set does not contain it...
                boolean socketExists = false;
                for (final ConnectionAddress connectionAddress : connectionAddresses) {
                    if (addressStatus != null) {
                        connectionAddress.status = addressStatus;
                        Logger.debug("Updated Status: " + connectionAddress);
                    }

                    if (Util.areEqual(jsonSocket, connectionAddress.connection.get())) {
                        socketExists = true;
                    }
                }

                if (! socketExists) {
                    final ConnectionAddress connectionAddress = new ConnectionAddress(addressKey, jsonSocket);
                    connectionAddress.status = addressStatus;
                    Logger.debug("Updated Status: " + connectionAddress);

                    connectionAddresses.add(connectionAddress);
                }
            }
        }
    }

    protected Boolean _removeAddressSubscription(final AddressSubscriptionKey addressKey, final JsonSocket jsonSocket) {
        synchronized (_connectionAddresses) {
            final LinkedList<ConnectionAddress> connectionAddresses = _connectionAddresses.get(addressKey);
            if (connectionAddresses == null) { return false; }

            final Iterator<ConnectionAddress> iterator = connectionAddresses.iterator();
            while (iterator.hasNext()) {
                final ConnectionAddress connectionAddress = iterator.next();
                if (Util.areEqual(jsonSocket, connectionAddress.connection.get())) {
                    iterator.remove();
                    return true;
                }
            }

            return false;
        }
    }

    protected void _handleSubscribeScriptHashMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final String addressHashString = paramsJson.getString(0);
        final Sha256Hash scriptHash = Sha256Hash.fromHexString(BitcoinUtil.reverseEndianString(addressHashString));
        if (scriptHash == null) {
            Logger.debug("Invalid Address hash: " + addressHashString);
            return;
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(scriptHash, addressHashString);
        final Sha256Hash addressStatus;
        {
            final Sha256Hash cachedStatus = _getCachedAddressStatus(addressKey);
            if (cachedStatus != null) {
                addressStatus = cachedStatus;
            }
            else {
                addressStatus = _calculateAddressStatus(addressKey);
            }
        }

        _addAddressSubscription(addressKey, jsonSocket, addressStatus);

        {
            final Json json = new ElectrumJson(false);
            json.put("id", id);
            json.put("result", addressStatus);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }
    }

    protected void _handleUnsubscribeScriptHashMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final String addressHashString = paramsJson.getString(0);
        final Sha256Hash scriptHash = Sha256Hash.fromHexString(BitcoinUtil.reverseEndianString(addressHashString));
        if (scriptHash == null) {
            Logger.debug("Invalid Address hash: " + addressHashString);
            return;
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(scriptHash, addressHashString);
        final Boolean addressExisted = _removeAddressSubscription(addressKey, jsonSocket);

        {
            final Json json = new ElectrumJson(false);
            json.put("id", id);
            json.put("result", addressExisted);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }
    }

    protected void _handleSubscribeAddressMessage(final JsonSocket jsonSocket, final Json message) {
        final AddressInflater addressInflater = new AddressInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final String addressString = paramsJson.getString(0);
        final Address address = Util.coalesce(addressInflater.fromBase32Check(addressString), addressInflater.fromBase58Check(addressString));
        if (address == null) {
            Logger.debug("Invalid address: " + addressString);
            return;
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(address, addressString);
        final Sha256Hash addressStatus;
        {
            final Sha256Hash cachedStatus = _getCachedAddressStatus(addressKey);
            if (cachedStatus != null) {
                addressStatus = cachedStatus;
            }
            else {
                addressStatus = _calculateAddressStatus(addressKey);
            }
        }

        _addAddressSubscription(addressKey, jsonSocket, addressStatus);

        {
            final Json json = new ElectrumJson(false);
            json.put("id", id);
            json.put("result", addressStatus);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }
    }

    protected void _handleGetBalanceMessage(final JsonSocket jsonSocket, final AddressSubscriptionKey addressKey, final Integer requestId) {
        final Long confirmedBalance;
        final Long unconfirmedBalance;
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            final Json balanceJson = nodeConnection.getAddressBalance(addressKey.scriptHash);
            unconfirmedBalance = balanceJson.getLong("balance");
            confirmedBalance = balanceJson.getLong("confirmedBalance");
        }

        final Json balanceJson = new ElectrumJson(false);
        balanceJson.put("confirmed", confirmedBalance);
        balanceJson.put("unconfirmed", unconfirmedBalance);

        {
            final Json json = new ElectrumJson(false);
            json.put("id", requestId);
            json.put("result", balanceJson);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }

        jsonSocket.flush();
    }

    protected void _handleGetScriptHashBalanceMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final String addressHashString = paramsJson.getString(0);
        final Sha256Hash scriptHash = Sha256Hash.fromHexString(BitcoinUtil.reverseEndianString(addressHashString));
        if (scriptHash == null) {
            Logger.debug("Invalid Address hash: " + addressHashString);
            return;
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(scriptHash, addressHashString);
        _handleGetBalanceMessage(jsonSocket, addressKey, id);
    }

    protected void _handleGetAddressBalanceMessage(final JsonSocket jsonSocket, final Json message) {
        final AddressInflater addressInflater = new AddressInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final String addressString = paramsJson.getString(0);
        final Address address = Util.coalesce(addressInflater.fromBase32Check(addressString), addressInflater.fromBase58Check(addressString));
        if (address == null) {
            Logger.debug("Invalid address: " + addressString);
            return;
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(address, addressString);
        _handleGetBalanceMessage(jsonSocket, addressKey, id);
    }

    protected void _handleUnsubscribeAddressMessage(final JsonSocket jsonSocket, final Json message) {
        final AddressInflater addressInflater = new AddressInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        final String addressString = paramsJson.getString(0);
        final Address address = Util.coalesce(addressInflater.fromBase32Check(addressString), addressInflater.fromBase58Check(addressString));
        if (address == null) {
            Logger.debug("Invalid address: " + addressString);
            return;
        }

        final AddressSubscriptionKey addressKey = new AddressSubscriptionKey(address, addressString);
        final Boolean addressExisted = _removeAddressSubscription(addressKey, jsonSocket);

        {
            final Json json = new ElectrumJson(false);
            json.put("id", id);
            json.put("result", addressExisted);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }
    }

    protected void _handleGetAddressHistory(final JsonSocket jsonSocket, final Json message, final Boolean includeConfirmedTransactions, final Boolean includeUnconfirmedTransactions, final Boolean includeTransactionFees) {
        final TransactionInflater transactionInflater = new TransactionInflater();
        final AddressInflater addressInflater = new AddressInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        if (paramsJson.length() != 1) { return; }

        final Sha256Hash scriptHash;
        final Address address;
        {
            final String addressString = paramsJson.getString(0);
            scriptHash = Sha256Hash.fromHexString(BitcoinUtil.reverseEndianString(addressString));
            address = Util.coalesce(addressInflater.fromBase32Check(addressString), addressInflater.fromBase58Check(addressString));
            if ((scriptHash == null) && (address == null)) {
                Logger.debug("Invalid Address/Hash: " + addressString);
                return;
            }
        }

        final Json resultJson = new ElectrumJson(true);
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            final Json addressTransactionsJson;
            if (address != null) {
                addressTransactionsJson = nodeConnection.getAddressTransactions(address, true);
            }
            else {
                addressTransactionsJson = nodeConnection.getAddressTransactions(scriptHash, true);
            }

            final Json transactionsJson = addressTransactionsJson.get("transactions");
            final int transactionCount = transactionsJson.length();
            final MutableList<TransactionPosition> transactionPositions = new MutableList<>(transactionCount);
            for (int i = 0; i < transactionCount; ++i) {
                final String transactionHex = transactionsJson.getString(i);
                final Transaction transaction = transactionInflater.fromBytes(ByteArray.fromHexString(transactionHex));
                final Sha256Hash transactionHash = transaction.getHash();

                final Json transactionBlockHeightJson = nodeConnection.getTransactionBlockHeight(transactionHash);
                final Long blockHeight = transactionBlockHeightJson.getOrNull("blockHeight", Json.Types.LONG);
                final Integer transactionIndex = transactionBlockHeightJson.getOrNull("transactionIndex", Json.Types.INTEGER);
                final Boolean hasUnconfirmedInputs = transactionBlockHeightJson.getBoolean("hasUnconfirmedInputs");

                final TransactionPosition transactionPosition = new TransactionPosition(blockHeight, transactionIndex, hasUnconfirmedInputs, transactionHash);

                if ( transactionPosition.isUnconfirmedTransaction() && (! includeUnconfirmedTransactions) ) { continue; }
                if ( (! transactionPosition.isUnconfirmedTransaction()) && (! includeConfirmedTransactions) ) { continue; }

                transactionPositions.add(transactionPosition);
            }
            transactionPositions.sort(TransactionPosition.COMPARATOR);

            if (includeTransactionFees) {
                for (final TransactionPosition transactionPosition : transactionPositions) {
                    final Json getTransactionJson = nodeConnection.getTransaction(transactionPosition.transactionHash, false);
                    final Json transactionJson = getTransactionJson.get("transaction");
                    transactionPosition.transactionFee = transactionJson.getLong("fee");
                }
            }

            for (final TransactionPosition transactionPosition : transactionPositions) {
                resultJson.add(transactionPosition);
            }
        }

        {
            final Json json = new ElectrumJson(false);
            json.put("id", id);
            json.put("result", resultJson);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }
        jsonSocket.flush();
    }

    protected void _handleGetUnspentOutputs(final JsonSocket jsonSocket, final Json message) {
        final AddressInflater addressInflater = new AddressInflater();

        final Integer id = message.getInteger("id");
        final Json paramsJson = message.get("params");

        if (paramsJson.length() != 1) { return; }

        final Sha256Hash scriptHash;
        final Address address;
        {
            final String addressString = paramsJson.getString(0);
            address = Util.coalesce(addressInflater.fromBase32Check(addressString), addressInflater.fromBase58Check(addressString));
            if (address != null) {
                scriptHash = ScriptBuilder.computeScriptHash(address);
            }
            else {
                scriptHash = Sha256Hash.fromHexString(BitcoinUtil.reverseEndianString(addressString));
                if (scriptHash == null) {
                    Logger.debug("Invalid Address/Hash: " + addressString);
                    return;
                }
            }
        }

        final Json resultJson = new ElectrumJson(true);
        try (final NodeJsonRpcConnection nodeConnection = _getNodeConnection()) {
            nodeConnection.enableKeepAlive(true);

            final Json addressTransactionsJson;
            if (address != null) {
                addressTransactionsJson = nodeConnection.getAddressTransactions(address, false);
            }
            else {
                addressTransactionsJson = nodeConnection.getAddressTransactions(scriptHash, false);
            }

            final Json transactionsJson = addressTransactionsJson.get("transactions");
            final int transactionCount = transactionsJson.length();
            Logger.debug("Tx Count: " + transactionCount);
            final MutableList<TransactionPosition> transactionPositions = new MutableList<>(transactionCount);
            final HashMap<TransactionPosition, MutableList<Json>> transactionPositionJsons = new HashMap<>();
            for (int i = 0; i < transactionCount; ++i) {
                final Json transactionJson = transactionsJson.get(i);
                final Sha256Hash transactionHash = Sha256Hash.fromHexString(transactionJson.getString("hash"));

                final Json transactionBlockHeightJson = nodeConnection.getTransactionBlockHeight(transactionHash);
                final Long blockHeight = transactionBlockHeightJson.getOrNull("blockHeight", Json.Types.LONG);
                final Integer transactionIndex = transactionBlockHeightJson.getOrNull("transactionIndex", Json.Types.INTEGER);
                final Boolean hasUnconfirmedInputs = transactionBlockHeightJson.getBoolean("hasUnconfirmedInputs");

                final TransactionPosition transactionPosition = new TransactionPosition(blockHeight, transactionIndex, hasUnconfirmedInputs, transactionHash);
                transactionPositions.add(transactionPosition);

                final Json transactionOutputsJson = transactionJson.get("outputs");
                final int transactionOutputCount = transactionOutputsJson.length();
                for (int outputIndex = 0; outputIndex < transactionOutputCount; ++outputIndex) {
                    final Json transactionOutputJson = transactionOutputsJson.get(outputIndex);
                    final Long amount = transactionOutputJson.getLong("amount");
                    final Sha256Hash spentByTransaction = Sha256Hash.fromHexString(transactionOutputJson.getOrNull("spentByTransaction", Json.Types.STRING));
                    final Sha256Hash transactionOutputScriptHash = Sha256Hash.fromHexString(transactionOutputJson.getOrNull("scriptHash", Json.Types.STRING));

                    if (! Util.areEqual(scriptHash, transactionOutputScriptHash)) { continue; }

                    final boolean isUnspent = (spentByTransaction == null);
                    if (! isUnspent) { continue; }

                    final Json json = transactionPosition.toJson();
                    json.put("tx_pos", outputIndex);
                    json.put("value", amount);

                    MutableList<Json> jsonList = transactionPositionJsons.get(transactionPosition);
                    if (jsonList == null) {
                        jsonList = new MutableList<>();
                        transactionPositionJsons.put(transactionPosition, jsonList);
                    }

                    jsonList.add(json);
                }
            }
            transactionPositions.sort(TransactionPosition.COMPARATOR);

            // Add the TransactionOutput Json objects in sorted order by their associated TransactionPosition...
            for (final TransactionPosition transactionPosition : transactionPositions) {
                final List<Json> jsonList = transactionPositionJsons.get(transactionPosition);
                if (jsonList == null) { continue; }

                for (final Json transactionPositionJson : jsonList) {
                    resultJson.add(transactionPositionJson);
                }
            }
        }

        {
            final Json json = new ElectrumJson(false);
            json.put("id", id);
            json.put("result", resultJson);

            jsonSocket.write(new JsonProtocolMessage(json));
            Logger.debug("Wrote: " + json);
            jsonSocket.flush();
        }
        jsonSocket.flush();
    }

    protected void _handlePeersMessage(final JsonSocket jsonSocket, final Json message) {
        final Integer id = message.getInteger("id");

        final Json json = new ElectrumJson(false);

        json.put("id", id);
        json.put("result", new ElectrumJson(true));
        jsonSocket.write(new JsonProtocolMessage(json));
        Logger.debug("Wrote: " + json);
        jsonSocket.flush();
    }

    protected void _onConnect(final JsonSocket jsonSocket) {
        Logger.info("Electrum Socket Connected: " + jsonSocket.getIp() + ":" + jsonSocket.getPort());

        jsonSocket.setMessageReceivedCallback(new Runnable() {
            @Override
            public synchronized void run() { // Intentionally allow only one-message at a time to force message FIFO processing.
                final JsonProtocolMessage jsonProtocolMessage = jsonSocket.popMessage();
                if (jsonProtocolMessage == null) { return; }

                final Json jsonMessage = jsonProtocolMessage.getMessage();
                Logger.debug("Received: " + jsonMessage);

                final String method = jsonMessage.getString("method");
                switch (method) {
                    case "server.version": {
                        _handleServerVersionMessage(jsonSocket, jsonMessage);
                    } break;
                    case "server.banner": {
                        _handleBannerMessage(jsonSocket, jsonMessage);
                    } break;
                    case "server.donation_address": {
                        _handleDonationAddressMessage(jsonSocket, jsonMessage);
                    } break;
                    case "server.ping": {
                        _handlePingMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.relayfee": {
                        _handleMinimumRelayFeeMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.estimatefee": {
                        _handleEstimateFeeMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.headers.subscribe": {
                        _handleSubscribeBlockHeadersMessage(jsonSocket, jsonMessage);
                    } break;
                    case "server.peers.subscribe": {
                        _handlePeersMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.scripthash.subscribe": {
                        _handleSubscribeScriptHashMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.address.subscribe": {
                        _handleSubscribeAddressMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.scripthash.unsubscribe": {
                        _handleUnsubscribeScriptHashMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.scripthash.get_balance": {
                        _handleGetScriptHashBalanceMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.address.get_balance": {
                        _handleGetAddressBalanceMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.address.unsubscribe": {
                        _handleUnsubscribeAddressMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.address.get_history":
                    case "blockchain.scripthash.get_history": {
                        _handleGetAddressHistory(jsonSocket, jsonMessage, true, true, false);
                    } break;
                    case "blockchain.address.get_mempool":
                    case "blockchain.scripthash.get_mempool": {
                        _handleGetAddressHistory(jsonSocket, jsonMessage, false, true, true);
                    } break;
                    case "blockchain.address.listunspent":
                    case "blockchain.scripthash.listunspent": {
                        _handleGetUnspentOutputs(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.transaction.broadcast": {
                        _handleSubmitTransactionMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.block.headers": {
                        _handleBlockHeadersMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.block.header": {
                        _handleBlockHeaderMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.transaction.get": {
                        _handleGetTransactionMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.transaction.get_merkle": {
                        _handleGetTransactionMerkleProofMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.address.get_scripthash": {
                        _handleConvertAddressToScriptHashMessage(jsonSocket, jsonMessage);
                    } break;
                    case "blockchain.transaction.id_from_pos": {
                        _handleGetTransactionFromBlockPositionMessage(jsonSocket, jsonMessage);
                    } break;
                    default: {
                        Logger.debug("Received unsupported message: " + jsonMessage);
                    }
                }
            }
        });

        final Long socketId = jsonSocket.getId();
        jsonSocket.setOnClosedCallback(new Runnable() {
            @Override
            public void run() {
                _connections.remove(socketId);
            }
        });
        _connections.put(socketId, jsonSocket);

        jsonSocket.beginListening();
    }

    protected void _onDisconnect(final JsonSocket jsonSocket) {
        Logger.debug("Electrum Socket Disconnected: " + jsonSocket.getIp() + ":" + jsonSocket.getPort());
        _connections.remove(jsonSocket.getId());
    }

    public ElectrumModule(final ElectrumProperties electrumProperties) {
        _electrumProperties = electrumProperties;
        _threadPool = new CachedThreadPool(1024, 30000L);

        _minTransactionFeePerByte = 1L;

        final Integer port = _electrumProperties.getHttpPort();
        final Integer tlsPort = _electrumProperties.getTlsPort();
        final TlsCertificate tlsCertificate;
        {
            final String certificateFile = _electrumProperties.getTlsCertificateFile();
            final String certificateKeyFile = _electrumProperties.getTlsKeyFile();
            tlsCertificate = _loadCertificate(certificateFile, certificateKeyFile);
        }

        _tlsIsEnabled = ((tlsCertificate != null) && (tlsPort != null));
        _electrumServerSocket = new ElectrumServerSocket(port, tlsPort, tlsCertificate, _threadPool);
        _electrumServerSocket.setSocketEventCallback(new ElectrumServerSocket.SocketEventCallback() {
            @Override
            public void onConnect(final JsonSocket socketConnection) {
                _onConnect(socketConnection);
            }

            @Override
            public void onDisconnect(final JsonSocket socketConnection) {
                _onDisconnect(socketConnection);
            }
        });

        _maintenanceThread = new Thread(new Runnable() {
            @Override
            public void run() {
                int iterationsSinceAddressCleanup = 0;
                final Thread thread = Thread.currentThread();
                while (! thread.isInterrupted()) {
                    try {
                        Thread.sleep(15000L);
                    }
                    catch (final InterruptedException exception) {
                        return;
                    }

                    iterationsSinceAddressCleanup += 1;

                    if (iterationsSinceAddressCleanup >= 20) {
                        synchronized (_connectionAddresses) {
                            for (final Map.Entry<AddressSubscriptionKey, LinkedList<ConnectionAddress>> entry : _connectionAddresses.entrySet()) {
                                boolean queueHasActiveSocket = false;

                                final LinkedList<ConnectionAddress> connectionAddresses = entry.getValue();
                                final Iterator<ConnectionAddress> iterator = connectionAddresses.iterator();
                                while (iterator.hasNext()) {
                                    final ConnectionAddress connectionAddress = iterator.next();

                                    final JsonSocket jsonSocket = connectionAddress.connection.get();
                                    final boolean jsonSocketIsConnected = ( (jsonSocket != null) && jsonSocket.isConnected() );

                                    if (! jsonSocketIsConnected) {
                                        iterator.remove();
                                    }
                                    else {
                                        queueHasActiveSocket = true;
                                    }
                                }

                                if (! queueHasActiveSocket) {
                                    _connectionAddresses.remove(entry.getKey());
                                }
                            }
                        }

                        iterationsSinceAddressCleanup = 0;
                    }

                    if ( (_nodeNotificationConnection == null) || (! _nodeNotificationConnection.isConnected()) ) {
                        _createNodeNotificationConnection();
                    }
                }
            }
        });
        _maintenanceThread.setName("Electrum Maintenance Thread");
        _maintenanceThread.setDaemon(true);
    }

    public void loop() {
        final Thread mainThread = Thread.currentThread();

        _threadPool.start();

        _cacheBlockHeaders();
        _createNodeNotificationConnection();

        _electrumServerSocket.start();

        final Integer port = _electrumProperties.getHttpPort();
        Logger.info("Listening on port: " + port);
        if (_tlsIsEnabled) {
            final Integer tlsPort = _electrumProperties.getTlsPort();
            Logger.info("Listening on port: " + tlsPort);
        }

        while (! mainThread.isInterrupted()) {
            try { Thread.sleep(10000L); } catch (final Exception exception) { }
        }

        _electrumServerSocket.stop();

        for (final JsonSocket socket : _connections.values()) {
            socket.close();
        }
        _connections.clear();

        _maintenanceThread.interrupt();
        try {
            _maintenanceThread.join(30000L);
        }
        catch (final Exception exception) { }

        if (_nodeNotificationConnection != null) {
            _nodeNotificationConnection.close();
            _nodeNotificationConnection = null;
        }

        _threadPool.stop();
    }
}
