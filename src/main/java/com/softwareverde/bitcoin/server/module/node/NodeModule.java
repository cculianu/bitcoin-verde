package com.softwareverde.bitcoin.server.module.node;

import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.bitcoin.server.Configuration;
import com.softwareverde.bitcoin.server.Constants;
import com.softwareverde.bitcoin.server.Environment;
import com.softwareverde.bitcoin.server.database.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.database.cache.MasterDatabaseManagerCache;
import com.softwareverde.bitcoin.server.database.cache.ReadOnlyLocalDatabaseManagerCache;
import com.softwareverde.bitcoin.server.database.cache.utxo.NativeUnspentTransactionOutputCache;
import com.softwareverde.bitcoin.server.database.pool.MysqlDatabaseConnectionPool;
import com.softwareverde.bitcoin.server.message.BitcoinProtocolMessage;
import com.softwareverde.bitcoin.server.module.CacheWarmer;
import com.softwareverde.bitcoin.server.module.DatabaseConfigurer;
import com.softwareverde.bitcoin.server.module.node.handler.InventoryMessageHandler;
import com.softwareverde.bitcoin.server.module.node.handler.MemoryPoolEnquirerHandler;
import com.softwareverde.bitcoin.server.module.node.handler.RequestDataHandler;
import com.softwareverde.bitcoin.server.module.node.handler.SynchronizationStatusHandler;
import com.softwareverde.bitcoin.server.module.node.handler.block.QueryBlockHeadersHandler;
import com.softwareverde.bitcoin.server.module.node.handler.block.QueryBlocksHandler;
import com.softwareverde.bitcoin.server.module.node.handler.transaction.OrphanedTransactionsCache;
import com.softwareverde.bitcoin.server.module.node.handler.transaction.TransactionInventoryMessageHandlerFactory;
import com.softwareverde.bitcoin.server.module.node.rpc.NodeHandler;
import com.softwareverde.bitcoin.server.module.node.rpc.QueryBalanceHandler;
import com.softwareverde.bitcoin.server.module.node.rpc.ShutdownHandler;
import com.softwareverde.bitcoin.server.module.node.sync.AddressProcessor;
import com.softwareverde.bitcoin.server.module.node.sync.BlockchainBuilder;
import com.softwareverde.bitcoin.server.module.node.sync.BlockDownloadRequester;
import com.softwareverde.bitcoin.server.module.node.sync.BlockHeaderDownloader;
import com.softwareverde.bitcoin.server.module.node.sync.block.BlockDownloader;
import com.softwareverde.bitcoin.server.node.BitcoinNode;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.mysql.MysqlDatabaseConnection;
import com.softwareverde.database.mysql.MysqlDatabaseConnectionFactory;
import com.softwareverde.database.mysql.embedded.DatabaseCommandLineArguments;
import com.softwareverde.database.mysql.embedded.DatabaseInitializer;
import com.softwareverde.database.mysql.embedded.EmbeddedMysqlDatabase;
import com.softwareverde.database.mysql.embedded.properties.DatabaseProperties;
import com.softwareverde.io.Logger;
import com.softwareverde.network.socket.BinarySocket;
import com.softwareverde.network.socket.BinarySocketServer;
import com.softwareverde.network.socket.JsonSocketServer;
import com.softwareverde.network.time.MutableNetworkTime;
import com.softwareverde.util.ByteUtil;

import java.io.File;

public class NodeModule {
    public static void execute(final String configurationFileName) {
        final NodeModule nodeModule = new NodeModule(configurationFileName);
        nodeModule.loop();
    }

    protected final Boolean _shouldWarmUpCache = true;

    protected final Configuration _configuration;
    protected final Environment _environment;

    protected final BitcoinNodeManager _nodeManager;
    protected final BinarySocketServer _socketServer;
    protected final JsonSocketServer _jsonRpcSocketServer;
    protected final BlockHeaderDownloader _blockHeaderDownloader;
    protected final BlockDownloader _blockDownloader;
    protected final BlockchainBuilder _blockchainBuilder;
    protected final AddressProcessor _addressProcessor;

    protected final NodeInitializer _nodeInitializer;
    protected final BanFilter _banFilter;
    protected final MutableNetworkTime _mutableNetworkTime = new MutableNetworkTime();

    protected final MutableList<MysqlDatabaseConnectionPool> _openDatabaseConnectionPools = new MutableList<MysqlDatabaseConnectionPool>();

    protected MysqlDatabaseConnectionPool _createDatabaseConnectionPool(final MysqlDatabaseConnectionFactory databaseConnectionFactory, final Integer maxCount) {
        final MysqlDatabaseConnectionPool databaseConnectionPool = new MysqlDatabaseConnectionPool(databaseConnectionFactory, maxCount);
        _openDatabaseConnectionPools.add(databaseConnectionPool);
        return databaseConnectionPool;
    }

    protected Configuration _loadConfigurationFile(final String configurationFilename) {
        final File configurationFile =  new File(configurationFilename);
        if (! configurationFile.isFile()) {
            Logger.error("Invalid configuration file.");
            BitcoinUtil.exitFailure();
        }

        return new Configuration(configurationFile);
    }

    protected void _warmUpCache() {
        Logger.log("[Warming Cache]");
        final CacheWarmer cacheWarmer = new CacheWarmer();
        final MasterDatabaseManagerCache masterDatabaseManagerCache = _environment.getMasterDatabaseManagerCache();
        final EmbeddedMysqlDatabase database = _environment.getDatabase();
        final MysqlDatabaseConnectionFactory databaseConnectionFactory = database.getDatabaseConnectionFactory();
        cacheWarmer.warmUpCache(masterDatabaseManagerCache, databaseConnectionFactory);
    }

    protected NodeModule(final String configurationFilename) {
        final Thread mainThread = Thread.currentThread();

        _configuration = _loadConfigurationFile(configurationFilename);

        final Configuration.ServerProperties serverProperties = _configuration.getServerProperties();
        final DatabaseProperties databaseProperties = _configuration.getDatabaseProperties();

        final EmbeddedMysqlDatabase database;
        {
            EmbeddedMysqlDatabase databaseInstance = null;
            try {
                final DatabaseInitializer databaseInitializer = new DatabaseInitializer("queries/init.sql", Constants.DATABASE_VERSION, new DatabaseInitializer.DatabaseUpgradeHandler() {
                    @Override
                    public Boolean onUpgrade(final int currentVersion, final int requiredVersion) { return false; }
                });

                final DatabaseCommandLineArguments commandLineArguments = new DatabaseCommandLineArguments();
                DatabaseConfigurer.configureCommandLineArguments(commandLineArguments, serverProperties);

                // databaseInstance = new DebugEmbeddedMysqlDatabase(databaseProperties, databaseInitializer, commandLineArguments);
                databaseInstance = new EmbeddedMysqlDatabase(databaseProperties, databaseInitializer, commandLineArguments);
            }
            catch (final DatabaseException exception) {
                Logger.log(exception);
            }
            database = databaseInstance;

            if (database != null) {
                Logger.log("[Database Online]");
            }
            else {
                BitcoinUtil.exitFailure();
            }
        }

        { // Initialize the NativeUnspentTransactionOutputCache...
            final Boolean nativeCacheIsEnabled = NativeUnspentTransactionOutputCache.isEnabled();
            if (nativeCacheIsEnabled) {
                NativeUnspentTransactionOutputCache.init();
            }
            else {
                Logger.log("NOTICE: NativeUtxoCache not enabled.");
            }
        }

        final MasterDatabaseManagerCache masterDatabaseManagerCache = new MasterDatabaseManagerCache();
        final ReadOnlyLocalDatabaseManagerCache readOnlyDatabaseManagerCache = new ReadOnlyLocalDatabaseManagerCache(masterDatabaseManagerCache);

        _environment = new Environment(database, masterDatabaseManagerCache);

        final MysqlDatabaseConnectionFactory databaseConnectionFactory = database.getDatabaseConnectionFactory();

        _banFilter = new BanFilter(databaseConnectionFactory);

        final MutableMedianBlockTime medianBlockTime;
        final MutableMedianBlockTime medianBlockHeaderTime;
        { // Initialize MedianBlockTime...
            {
                MutableMedianBlockTime newMedianBlockTime = null;
                MutableMedianBlockTime newMedianBlockHeaderTime = null;
                try (final MysqlDatabaseConnection databaseConnection = databaseConnectionFactory.newConnection()) {
                    final BlockHeaderDatabaseManager blockHeaderDatabaseManager = new BlockHeaderDatabaseManager(databaseConnection, readOnlyDatabaseManagerCache);
                    newMedianBlockTime = blockHeaderDatabaseManager.initializeMedianBlockTime();
                    newMedianBlockHeaderTime = blockHeaderDatabaseManager.initializeMedianBlockHeaderTime();
                }
                catch (final DatabaseException exception) {
                    Logger.log(exception);
                    BitcoinUtil.exitFailure();
                }
                medianBlockTime = newMedianBlockTime;
                medianBlockHeaderTime = newMedianBlockHeaderTime;
            }
        }

        final SynchronizationStatusHandler synchronizationStatusHandler = new SynchronizationStatusHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);
        final MemoryPoolEnquirer memoryPoolEnquirer = new MemoryPoolEnquirerHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);

        final InventoryMessageHandler inventoryMessageHandler;
        {
            final Integer maxConnectionCount = Integer.MAX_VALUE; // (serverProperties.getMaxPeerCount() * 2);
            // final MysqlDatabaseConnectionPool databaseConnectionPool = _createDatabaseConnectionPool(databaseConnectionFactory, maxConnectionCount);
            inventoryMessageHandler = new InventoryMessageHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);
        }

        final OrphanedTransactionsCache orphanedTransactionsCache = new OrphanedTransactionsCache(readOnlyDatabaseManagerCache);

        { // Initialize NodeInitializer...
            final TransactionInventoryMessageHandlerFactory transactionsAnnouncementCallbackFactory = new TransactionInventoryMessageHandlerFactory(databaseConnectionFactory, readOnlyDatabaseManagerCache, orphanedTransactionsCache, _mutableNetworkTime, medianBlockTime);
            final QueryBlocksHandler queryBlocksHandler = new QueryBlocksHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);
            final QueryBlockHeadersHandler queryBlockHeadersHandler = new QueryBlockHeadersHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);
            final RequestDataHandler requestDataHandler = new RequestDataHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);
            _nodeInitializer = new NodeInitializer(synchronizationStatusHandler, inventoryMessageHandler, transactionsAnnouncementCallbackFactory, queryBlocksHandler, queryBlockHeadersHandler, requestDataHandler);
        }

        { // Initialize NodeManager...
            final Integer maxPeerCount = (serverProperties.skipNetworking() ? 0 : serverProperties.getMaxPeerCount());
            _nodeManager = new BitcoinNodeManager(maxPeerCount, databaseConnectionFactory, readOnlyDatabaseManagerCache, _mutableNetworkTime, _nodeInitializer, _banFilter, memoryPoolEnquirer, synchronizationStatusHandler);
        }

        final BlockProcessor blockProcessor;
        { // Initialize BlockSynchronizer...
            blockProcessor = new BlockProcessor(databaseConnectionFactory, masterDatabaseManagerCache, _mutableNetworkTime, medianBlockTime, orphanedTransactionsCache);
            blockProcessor.setMaxThreadCount(serverProperties.getMaxThreadCount());
            blockProcessor.setTrustedBlockHeight(serverProperties.getTrustedBlockHeight());
        }

        { // Initialize the BlockDownloader...
            // final Integer maxConnectionCount = Integer.MAX_VALUE; // (serverProperties.getMaxPeerCount() * 2);
            // final MysqlDatabaseConnectionPool databaseConnectionPool = _createDatabaseConnectionPool(databaseConnectionFactory, maxConnectionCount);
            _blockDownloader = new BlockDownloader(_nodeManager, databaseConnectionFactory, readOnlyDatabaseManagerCache);
        }

        final BlockDownloadRequester blockDownloadRequester = new BlockDownloadRequester(databaseConnectionFactory, _blockDownloader);

        { // Initialize BlockHeaderDownloader...
            _blockHeaderDownloader = new BlockHeaderDownloader(databaseConnectionFactory, readOnlyDatabaseManagerCache, _nodeManager, medianBlockHeaderTime, blockDownloadRequester);
        }

        {
            // final Integer maxConnectionCount = Integer.MAX_VALUE; // (serverProperties.getMaxPeerCount() * 2);
            // final MysqlDatabaseConnectionPool databaseConnectionPool = _createDatabaseConnectionPool(databaseConnectionFactory, maxConnectionCount);
            _blockchainBuilder = new BlockchainBuilder(_nodeManager, databaseConnectionFactory, readOnlyDatabaseManagerCache, blockProcessor, _blockDownloader.getStatusMonitor(), blockDownloadRequester);
        }

        _addressProcessor = new AddressProcessor(databaseConnectionFactory, readOnlyDatabaseManagerCache);

        { // Set the synchronization elements to cascade to each component...
            _blockchainBuilder.setNewBlockProcessedCallback(new BlockchainBuilder.NewBlockProcessedCallback() {
                @Override
                public void onNewBlock(final Long blockHeight) {
                    _addressProcessor.wakeUp();

                    final Long blockHeaderDownloaderBlockHeight = _blockHeaderDownloader.getBlockHeight();
                    if (blockHeaderDownloaderBlockHeight <= blockHeight) {
                        _blockHeaderDownloader.wakeUp();
                    }
                }
            });

            _blockHeaderDownloader.setNewBlockHeaderAvailableCallback(new Runnable() {
                @Override
                public void run() {
                    _blockDownloader.wakeUp();
                }
            });

            _blockDownloader.setNewBlockAvailableCallback(new Runnable() {
                @Override
                public void run() {
                    _blockchainBuilder.wakeUp();
                }
            });

            inventoryMessageHandler.setNewBlockHashesCallback(new Runnable() {
                @Override
                public void run() {
                    _blockDownloader.wakeUp();
                }
            });
        }

        _socketServer = new BinarySocketServer(serverProperties.getBitcoinPort(), BitcoinProtocolMessage.BINARY_PACKET_FORMAT);
        _socketServer.setSocketConnectedCallback(new BinarySocketServer.SocketConnectedCallback() {
            @Override
            public void run(final BinarySocket binarySocket) {
                final String host = binarySocket.getHost();

                final Boolean isBanned = _banFilter.isHostBanned(host);
                if (isBanned) {
                    binarySocket.close();
                    return;
                }

                final Boolean shouldBan = _banFilter.shouldBanHost(host);
                if (shouldBan) {
                    _banFilter.banHost(host);
                    binarySocket.close();
                    return;
                }

                Logger.log("New Connection: " + binarySocket);
                final BitcoinNode node = _nodeInitializer.initializeNode(binarySocket);
                _nodeManager.addNode(node);
            }
        });

        final Integer rpcPort = _configuration.getServerProperties().getBitcoinRpcPort();
        if (rpcPort > 0) {

            final JsonRpcSocketServerHandler.StatisticsContainer statisticsContainer = new JsonRpcSocketServerHandler.StatisticsContainer();
            { // Initialize statistics container...
                statisticsContainer.averageBlockHeadersPerSecond = _blockHeaderDownloader.getAverageBlockHeadersPerSecondContainer();
                statisticsContainer.averageBlocksPerSecond = blockProcessor.getAverageBlocksPerSecondContainer();
                statisticsContainer.averageTransactionsPerSecond = blockProcessor.getAverageTransactionsPerSecondContainer();
            }

            final JsonRpcSocketServerHandler.ShutdownHandler shutdownHandler = new ShutdownHandler(mainThread, _blockHeaderDownloader, _blockDownloader, _blockchainBuilder);
            final JsonRpcSocketServerHandler.NodeHandler nodeHandler = new NodeHandler(_nodeManager, _nodeInitializer);
            final JsonRpcSocketServerHandler.QueryBalanceHandler queryBalanceHandler = new QueryBalanceHandler(databaseConnectionFactory, readOnlyDatabaseManagerCache);

            final JsonSocketServer jsonRpcSocketServer = new JsonSocketServer(rpcPort);

            final JsonRpcSocketServerHandler rpcSocketServerHandler = new JsonRpcSocketServerHandler(_environment, synchronizationStatusHandler, statisticsContainer);
            rpcSocketServerHandler.setShutdownHandler(shutdownHandler);
            rpcSocketServerHandler.setNodeHandler(nodeHandler);
            rpcSocketServerHandler.setQueryBalanceHandler(queryBalanceHandler);

            jsonRpcSocketServer.setSocketConnectedCallback(rpcSocketServerHandler);
            _jsonRpcSocketServer = jsonRpcSocketServer;
        }
        else {
            _jsonRpcSocketServer = null;
        }
    }

    public void loop() {
        if (_shouldWarmUpCache) {
            _warmUpCache();
        }

        final Configuration.ServerProperties serverProperties = _configuration.getServerProperties();

        if (! serverProperties.skipNetworking()) {
            _nodeManager.startNodeMaintenanceThread();

            Logger.log("[Server Online]");

            for (final Configuration.SeedNodeProperties seedNodeProperties : serverProperties.getSeedNodeProperties()) {
                final String host = seedNodeProperties.getAddress();
                final Integer port = seedNodeProperties.getPort();

                final BitcoinNode node = _nodeInitializer.initializeNode(host, port);
                _nodeManager.addNode(node);
            }
        }
        else {
            Logger.log("[Skipping Networking]");
        }

        if (_jsonRpcSocketServer != null) {
            _jsonRpcSocketServer.start();
            Logger.log("[RPC Server Online]");
        }
        else {
            Logger.log("NOTICE: Bitcoin RPC Server not started.");
        }

        _socketServer.start();
        Logger.log("[Listening For Connections]");

        if (! serverProperties.skipNetworking()) {
            _blockHeaderDownloader.start();
            _blockDownloader.start();
            _blockchainBuilder.start();
            _addressProcessor.start();
            Logger.log("[Started Syncing Headers]");
        }

        while (! Thread.currentThread().isInterrupted()) {
            try { Thread.sleep(5000); } catch (final Exception e) { break; }
        }

        _addressProcessor.stop();
        _blockchainBuilder.stop();
        _blockDownloader.stop();
        _blockHeaderDownloader.stop();

        _nodeManager.shutdown();
        _nodeManager.stopNodeMaintenanceThread();
        _socketServer.stop();
        _banFilter.close();

        if (_jsonRpcSocketServer != null) {
            _jsonRpcSocketServer.stop();
        }

        while (! _openDatabaseConnectionPools.isEmpty()) {
            final MysqlDatabaseConnectionPool databaseConnectionPool = _openDatabaseConnectionPools.remove(0);
            databaseConnectionPool.close();
        }

        _environment.getMasterDatabaseManagerCache().close();

        System.exit(0);
    }
}
