package com.softwareverde.bitcoin.server.module;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.inflater.BlockHeaderInflaters;
import com.softwareverde.bitcoin.inflater.BlockInflaters;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.server.Environment;
import com.softwareverde.bitcoin.server.configuration.BitcoinProperties;
import com.softwareverde.bitcoin.server.configuration.CheckpointConfiguration;
import com.softwareverde.bitcoin.server.database.Database;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.DatabaseConnectionFactory;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.store.PendingBlockStore;
import com.softwareverde.bitcoin.server.module.node.store.PendingBlockStoreCore;
import com.softwareverde.bitcoin.server.module.node.store.UtxoCommitmentStore;
import com.softwareverde.bitcoin.server.module.node.store.UtxoCommitmentStoreCore;
import com.softwareverde.bitcoin.server.properties.DatabasePropertiesStore;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.script.locking.LockingScript;
import com.softwareverde.bitcoin.transaction.script.opcode.Operation;
import com.softwareverde.bitcoin.transaction.script.opcode.PushOperation;
import com.softwareverde.bitcoin.transaction.script.stack.Value;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.constable.list.List;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MilliTimer;

public class ChainValidationModule {
    protected final BitcoinProperties _bitcoinProperties;
    protected final Environment _environment;
    protected final DatabasePropertiesStore _propertiesStore;
    protected final Sha256Hash _startingBlockHash;
    protected final PendingBlockStore _blockStore;
    protected final UtxoCommitmentStore _utxoCommitmentStore;
    protected final CheckpointConfiguration _checkpointConfiguration;

    public ChainValidationModule(final BitcoinProperties bitcoinProperties, final Environment environment, final String startingBlockHash) {
        _bitcoinProperties = bitcoinProperties;
        _environment = environment;

        final DatabaseConnectionFactory databaseConnectionFactory = _environment.getDatabaseConnectionFactory();
        _propertiesStore = new DatabasePropertiesStore(databaseConnectionFactory);

        final MasterInflater masterInflater = new CoreInflater();
        _startingBlockHash = Util.coalesce(Sha256Hash.fromHexString(startingBlockHash), BlockHeader.GENESIS_BLOCK_HASH);

        { // Initialize the BlockStore...
            final String dataDirectory = bitcoinProperties.getDataDirectory();
            final BlockHeaderInflaters blockHeaderInflaters = masterInflater;
            final BlockInflaters blockInflaters = masterInflater;
            _blockStore = new PendingBlockStoreCore(dataDirectory, blockHeaderInflaters, blockInflaters) {
                @Override
                protected void _deletePendingBlockData(final String blockPath) {
                    if (bitcoinProperties.isDeletePendingBlocksEnabled()) {
                        super._deletePendingBlockData(blockPath);
                    }
                }
            };
        }

        { // Initialize the UtxoCommitmentStore...
            final String dataDirectory = bitcoinProperties.getDataDirectory();
            _utxoCommitmentStore = new UtxoCommitmentStoreCore(dataDirectory);
        }

        _checkpointConfiguration = new CheckpointConfiguration();
    }

    public void run() {
        final Thread mainThread = Thread.currentThread();
        mainThread.setPriority(Thread.MAX_PRIORITY);

        final Database database = _environment.getDatabase();
        _propertiesStore.start();

        final MasterInflater masterInflater = new CoreInflater();

        Sha256Hash nextBlockHash = _startingBlockHash;
        try (final DatabaseConnection databaseConnection = database.newConnection()) {
            final FullNodeDatabaseManager databaseManager = new FullNodeDatabaseManager(
                databaseConnection,
                database.getMaxQueryBatchSize(),
                _propertiesStore,
                _blockStore,
                _utxoCommitmentStore,
                masterInflater,
                _checkpointConfiguration,
                _bitcoinProperties.getMaxCachedUtxoCount(),
                _bitcoinProperties.getUtxoCachePurgePercent()
            );

            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();

            final BlockchainSegmentId headBlockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
            final Long maxBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);

            Long processedTransactionCount = 0L;
            final Long startTime = System.currentTimeMillis();
            while (true) {
                final Sha256Hash blockHash = nextBlockHash;

                final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(nextBlockHash);
                final Long blockHeight = blockHeaderDatabaseManager.getBlockHeight(blockId);

                final int percentComplete = (int) ((blockHeight * 100) / maxBlockHeight.floatValue());

                if (blockHeight % (maxBlockHeight / 100) == 0) {
                    final int secondsElapsed;
                    final float blocksPerSecond;
                    final float transactionsPerSecond;
                    {
                        final Long now = System.currentTimeMillis();
                        final int seconds = (int) ((now - startTime) / 1000L);
                        final long blockCount = blockHeight;
                        blocksPerSecond = (blockCount / (seconds + 1F));
                        secondsElapsed = seconds;
                        transactionsPerSecond = (processedTransactionCount / (seconds + 1F));
                    }

                    Logger.info(percentComplete + "% complete. " + blockHeight + " of " + maxBlockHeight + " - " + blockHash + " ("+ String.format("%.2f", blocksPerSecond) +" bps) (" + String.format("%.2f", transactionsPerSecond) + " tps) ("+ StringUtil.formatNumberString(secondsElapsed) +" seconds)");
                }

                final MilliTimer blockInflaterTimer = new MilliTimer();
                blockInflaterTimer.start();

                final Block block = _blockStore.getBlock(blockHash, blockHeight);;

                blockInflaterTimer.stop();
                Logger.debug("Block Inflation: " +  block.getHash() + " " + blockInflaterTimer.getMillisecondsElapsed() + "ms");

                for (final Transaction transaction : block.getTransactions()) {
                    for (final TransactionOutput transactionOutput : transaction.getTransactionOutputs()) {
                        final LockingScript lockingScript = transactionOutput.getLockingScript();
                        final List<Operation> operations = lockingScript.getOperations();

                        // TODO:
                        if (operations.getCount() != 3) { continue; }

                        final Operation firstOperation = operations.get(0);
                        if (firstOperation.getType() != Operation.Type.OP_PUSH) { continue; }
                        final PushOperation pushOperation = (PushOperation) firstOperation;
                        final Value value = pushOperation.getValue();
                        // ...
                    }
                }

                processedTransactionCount += block.getTransactionCount();

                nextBlockHash = null;
                final BlockId nextBlockId = blockHeaderDatabaseManager.getChildBlockId(headBlockchainSegmentId, blockId);
                if (nextBlockId == null) { break; }

                final Boolean nextBlockHasTransactions = blockDatabaseManager.hasTransactions(nextBlockId);
                if (nextBlockHasTransactions) {
                    nextBlockHash = blockHeaderDatabaseManager.getBlockHash(nextBlockId);
                }
            }

            _propertiesStore.stop();
        }
        catch (final DatabaseException exception) {
            Logger.error("Last validated block: " + nextBlockHash, exception);
            BitcoinUtil.exitFailure();
        }

        System.exit(0);
    }
}
