package com.softwareverde.bitcoin.server.module.node.database.fullnode;

import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.server.configuration.CheckpointConfiguration;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.DatabaseConnectionFactory;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.utxo.UnspentTransactionOutputDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.store.PendingBlockStore;
import com.softwareverde.bitcoin.server.module.node.store.UtxoCommitmentStore;
import com.softwareverde.bitcoin.server.properties.PropertiesStore;
import com.softwareverde.database.DatabaseException;

public class FullNodeDatabaseManagerFactory implements DatabaseManagerFactory {
    protected final DatabaseConnectionFactory _databaseConnectionFactory;
    protected final Integer _maxQueryBatchSize;
    protected final PropertiesStore _propertiesStore;
    protected final PendingBlockStore _blockStore;
    protected final UtxoCommitmentStore _utxoCommitmentStore;
    protected final MasterInflater _masterInflater;
    protected final CheckpointConfiguration _checkpointConfiguration;
    protected final Long _maxUtxoCount;
    protected final Float _utxoPurgePercent;

    public FullNodeDatabaseManagerFactory(final DatabaseConnectionFactory databaseConnectionFactory, final Integer maxQueryBatchSize, final PropertiesStore propertiesStore, final PendingBlockStore blockStore, final UtxoCommitmentStore utxoCommitmentStore, final MasterInflater masterInflater, final CheckpointConfiguration checkpointConfiguration) {
        this(databaseConnectionFactory, maxQueryBatchSize, propertiesStore, blockStore, utxoCommitmentStore, masterInflater, checkpointConfiguration, UnspentTransactionOutputDatabaseManager.DEFAULT_MAX_UTXO_CACHE_COUNT, UnspentTransactionOutputDatabaseManager.DEFAULT_PURGE_PERCENT);
    }

    public FullNodeDatabaseManagerFactory(final DatabaseConnectionFactory databaseConnectionFactory, final Integer maxQueryBatchSize, final PropertiesStore propertiesStore, final PendingBlockStore blockStore, final UtxoCommitmentStore utxoCommitmentStore, final MasterInflater masterInflater, final CheckpointConfiguration checkpointConfiguration, final Long maxUtxoCount, final Float utxoPurgePercent) {
        _databaseConnectionFactory = databaseConnectionFactory;
        _maxQueryBatchSize = maxQueryBatchSize;
        _propertiesStore = propertiesStore;
        _blockStore = blockStore;
        _utxoCommitmentStore = utxoCommitmentStore;
        _masterInflater = masterInflater;
        _maxUtxoCount = maxUtxoCount;
        _utxoPurgePercent = utxoPurgePercent;
        _checkpointConfiguration = checkpointConfiguration;
    }

    @Override
    public FullNodeDatabaseManager newDatabaseManager() throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseConnectionFactory.newConnection();
        return new FullNodeDatabaseManager(databaseConnection, _maxQueryBatchSize, _propertiesStore, _blockStore, _utxoCommitmentStore, _masterInflater, _checkpointConfiguration, _maxUtxoCount, _utxoPurgePercent);
    }

    @Override
    public DatabaseConnectionFactory getDatabaseConnectionFactory() {
        return _databaseConnectionFactory;
    }

    @Override
    public FullNodeDatabaseManagerFactory newDatabaseManagerFactory(final DatabaseConnectionFactory databaseConnectionFactory) {
        return new FullNodeDatabaseManagerFactory(databaseConnectionFactory, _maxQueryBatchSize, _propertiesStore, _blockStore, _utxoCommitmentStore, _masterInflater, _checkpointConfiguration, _maxUtxoCount, _utxoPurgePercent);
    }

    @Override
    public Integer getMaxQueryBatchSize() {
        return _maxQueryBatchSize;
    }
}
