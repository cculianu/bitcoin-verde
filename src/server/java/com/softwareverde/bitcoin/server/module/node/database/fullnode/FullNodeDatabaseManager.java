package com.softwareverde.bitcoin.server.module.node.database.fullnode;

import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.chain.utxo.UtxoCommitmentManager;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.server.configuration.CheckpointConfiguration;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.module.node.database.BlockchainCacheReference;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.BlockchainCache;
import com.softwareverde.bitcoin.server.module.node.database.block.MutableBlockchainCache;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManagerCore;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManagerCore;
import com.softwareverde.bitcoin.server.module.node.database.indexer.BlockchainIndexerDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.indexer.BlockchainIndexerDatabaseManagerCore;
import com.softwareverde.bitcoin.server.module.node.database.node.fullnode.FullNodeBitcoinNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.node.fullnode.FullNodeBitcoinNodeDatabaseManagerCore;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.FullNodeTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.FullNodeTransactionDatabaseManagerCore;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.input.UnconfirmedTransactionInputDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.output.UnconfirmedTransactionOutputDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.utxo.UnspentTransactionOutputDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.utxo.UnspentTransactionOutputJvmManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.pending.PendingTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.slp.SlpTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.slp.SlpTransactionDatabaseManagerCore;
import com.softwareverde.bitcoin.server.module.node.database.utxo.UtxoCommitmentDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.store.PendingBlockStore;
import com.softwareverde.bitcoin.server.module.node.store.UtxoCommitmentStore;
import com.softwareverde.bitcoin.server.module.node.utxo.UtxoCommitmentManagerCore;
import com.softwareverde.bitcoin.server.properties.PropertiesStore;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.util.TransactionUtil;

public class FullNodeDatabaseManager implements DatabaseManager {
    protected final DatabaseConnection _databaseConnection;
    protected final PropertiesStore _propertiesStore;
    protected final Integer _maxQueryBatchSize;
    protected final PendingBlockStore _blockStore;
    protected final MasterInflater _masterInflater;
    protected final Long _maxUtxoCount;
    protected final Float _utxoPurgePercent;
    protected final CheckpointConfiguration _checkpointConfiguration;
    protected final UtxoCommitmentStore _utxoCommitmentStore;

    protected FullNodeBitcoinNodeDatabaseManager _nodeDatabaseManager;
    protected BlockchainDatabaseManagerCore _blockchainDatabaseManager;
    protected FullNodeBlockDatabaseManager _blockDatabaseManager;
    protected BlockHeaderDatabaseManagerCore _blockHeaderDatabaseManager;
    protected BlockchainIndexerDatabaseManager _blockchainIndexerDatabaseManager;
    protected FullNodeTransactionDatabaseManager _transactionDatabaseManager;
    protected UnconfirmedTransactionInputDatabaseManager _unconfirmedTransactionInputDatabaseManager;
    protected UnconfirmedTransactionOutputDatabaseManager _unconfirmedTransactionOutputDatabaseManager;
    protected PendingTransactionDatabaseManager _pendingTransactionDatabaseManager;
    protected SlpTransactionDatabaseManager _slpTransactionDatabaseManager;
    protected UnspentTransactionOutputDatabaseManager _unspentTransactionOutputDatabaseManager;
    protected UtxoCommitmentDatabaseManager _utxoCommitmentDatabaseManager;
    protected UtxoCommitmentManager _utxoCommitmentManager;

    protected Boolean _cacheWasMutated = false;
    protected final BlockchainCacheManager _blockchainCacheManager;
    protected final BlockchainCacheReference _blockchainCacheReference;
    protected MutableBlockchainCache _blockchainCache;

    protected Integer _getVersion() {
        final MutableBlockchainCache blockchainCache = _blockchainCacheManager.getBlockchainCache();
        if (blockchainCache == null) { return null; }
        return blockchainCache.getVersion();
    }

    public FullNodeDatabaseManager(final DatabaseConnection databaseConnection, final Integer maxQueryBatchSize, final PropertiesStore propertiesStore, final PendingBlockStore blockStore, final UtxoCommitmentStore utxoCommitmentStore, final MasterInflater masterInflater, final CheckpointConfiguration checkpointConfiguration, final BlockchainCacheManager blockchainCacheManager) {
        this(databaseConnection, maxQueryBatchSize, propertiesStore, blockStore, utxoCommitmentStore, masterInflater, checkpointConfiguration, UnspentTransactionOutputDatabaseManager.DEFAULT_MAX_UTXO_CACHE_COUNT, UnspentTransactionOutputDatabaseManager.DEFAULT_PURGE_PERCENT, blockchainCacheManager);
    }

    public FullNodeDatabaseManager(final DatabaseConnection databaseConnection, final Integer maxQueryBatchSize, final PropertiesStore propertiesStore, final PendingBlockStore blockStore, final UtxoCommitmentStore utxoCommitmentStore, final MasterInflater masterInflater, final CheckpointConfiguration checkpointConfiguration, final Long maxUtxoCount, final Float utxoPurgePercent, final BlockchainCacheManager blockchainCacheManager) {
        _databaseConnection = databaseConnection;
        _propertiesStore = propertiesStore;
        _maxQueryBatchSize = maxQueryBatchSize;
        _blockStore = blockStore;
        _masterInflater = masterInflater;
        _maxUtxoCount = maxUtxoCount;
        _utxoPurgePercent = utxoPurgePercent;
        _checkpointConfiguration = checkpointConfiguration;
        _utxoCommitmentStore = utxoCommitmentStore;

        _blockchainCacheManager = blockchainCacheManager;
        _blockchainCacheReference = new BlockchainCacheReference() {
            @Override
            public BlockchainCache getBlockchainCache() {
                if (_blockchainCache == null) {
                    _blockchainCache = blockchainCacheManager.getBlockchainCache();
                }

                return _blockchainCache;
            }

            @Override
            public MutableBlockchainCache getMutableBlockchainCache() {
                if (_blockchainCache == null) {
                    _blockchainCache = blockchainCacheManager.getBlockchainCache();
                }

                _cacheWasMutated = true;
                return _blockchainCache;
            }
        };
    }

    @Override
    public DatabaseConnection getDatabaseConnection() {
        return _databaseConnection;
    }

    @Override
    public FullNodeBitcoinNodeDatabaseManager getNodeDatabaseManager() {
        if (_nodeDatabaseManager == null) {
            _nodeDatabaseManager = new FullNodeBitcoinNodeDatabaseManagerCore(this);
        }

        return _nodeDatabaseManager;
    }

    @Override
    public BlockchainDatabaseManager getBlockchainDatabaseManager() {
        if (_blockchainDatabaseManager == null) {
            _blockchainDatabaseManager = new BlockchainDatabaseManagerCore(this, _blockchainCacheReference);
        }

        return _blockchainDatabaseManager;
    }

    @Override
    public FullNodeBlockDatabaseManager getBlockDatabaseManager() {
        if (_blockDatabaseManager == null) {
            _blockDatabaseManager = new FullNodeBlockDatabaseManager(this, _blockStore, _blockchainCacheReference);
        }

        return _blockDatabaseManager;
    }

    @Override
    public BlockHeaderDatabaseManager getBlockHeaderDatabaseManager() {
        if (_blockHeaderDatabaseManager == null) {
            _blockHeaderDatabaseManager = new BlockHeaderDatabaseManagerCore(this, _checkpointConfiguration, _blockchainCacheReference);
        }

        return _blockHeaderDatabaseManager;
    }

    @Override
    public FullNodeTransactionDatabaseManager getTransactionDatabaseManager() {
        if (_transactionDatabaseManager == null) {
            _transactionDatabaseManager = new FullNodeTransactionDatabaseManagerCore(this, _blockStore, _masterInflater);
        }

        return _transactionDatabaseManager;
    }

    @Override
    public Integer getMaxQueryBatchSize() {
        return _maxQueryBatchSize;
    }

    @Override
    public PropertiesStore getPropertiesStore() {
        return _propertiesStore;
    }

    public BlockchainIndexerDatabaseManager getBlockchainIndexerDatabaseManager() {
        if (_blockchainIndexerDatabaseManager == null) {
            final AddressInflater addressInflater = _masterInflater.getAddressInflater();
            _blockchainIndexerDatabaseManager = new BlockchainIndexerDatabaseManagerCore(addressInflater, this);
        }

        return _blockchainIndexerDatabaseManager;
    }

    public UnconfirmedTransactionInputDatabaseManager getUnconfirmedTransactionInputDatabaseManager() {
        if (_unconfirmedTransactionInputDatabaseManager == null) {
            _unconfirmedTransactionInputDatabaseManager = new UnconfirmedTransactionInputDatabaseManager(this);
        }

        return _unconfirmedTransactionInputDatabaseManager;
    }

    public UnconfirmedTransactionOutputDatabaseManager getUnconfirmedTransactionOutputDatabaseManager() {
        if (_unconfirmedTransactionOutputDatabaseManager == null) {
            _unconfirmedTransactionOutputDatabaseManager = new UnconfirmedTransactionOutputDatabaseManager(this);
        }

        return _unconfirmedTransactionOutputDatabaseManager;
    }

    public PendingTransactionDatabaseManager getPendingTransactionDatabaseManager() {
        if (_pendingTransactionDatabaseManager == null) {
            _pendingTransactionDatabaseManager = new PendingTransactionDatabaseManager(this);
        }

        return _pendingTransactionDatabaseManager;
    }

    public SlpTransactionDatabaseManager getSlpTransactionDatabaseManager() {
        if (_slpTransactionDatabaseManager == null) {
            _slpTransactionDatabaseManager = new SlpTransactionDatabaseManagerCore(this);
        }

        return _slpTransactionDatabaseManager;
    }

    public UnspentTransactionOutputDatabaseManager getUnspentTransactionOutputDatabaseManager() {
        if (_unspentTransactionOutputDatabaseManager == null) {
            _unspentTransactionOutputDatabaseManager = new UnspentTransactionOutputJvmManager(_maxUtxoCount, _utxoPurgePercent, this, _blockStore, _masterInflater);
        }

        return _unspentTransactionOutputDatabaseManager;
    }

    public UtxoCommitmentDatabaseManager getUtxoCommitmentDatabaseManager() {
        if (_utxoCommitmentDatabaseManager == null) {
            _utxoCommitmentDatabaseManager = new UtxoCommitmentDatabaseManager(this);
        }

        return _utxoCommitmentDatabaseManager;
    }

    public UtxoCommitmentManager getUtxoCommitmentManager() {
        if (_utxoCommitmentManager == null) {
            _utxoCommitmentManager = new UtxoCommitmentManagerCore(_databaseConnection, _utxoCommitmentStore);
        }

        return _utxoCommitmentManager;
    }

    @Override
    public void startTransaction() throws DatabaseException {
        TransactionUtil.startTransaction(_databaseConnection);
    }

    @Override
    public void commitTransaction() throws DatabaseException {
        TransactionUtil.commitTransaction(_databaseConnection);
        if (_cacheWasMutated) {
            _blockchainCacheManager.commitBlockchainCache(_blockchainCache);
            _cacheWasMutated = false;
        }
    }

    @Override
    public void rollbackTransaction() throws DatabaseException {
        TransactionUtil.rollbackTransaction(_databaseConnection);
        if (_cacheWasMutated) {
            _blockchainCacheManager.rollbackBlockchainCache();
            _cacheWasMutated = false;
            _blockchainCache = null;
        }
    }

    @Override
    public void close() throws DatabaseException {
        if (_cacheWasMutated) {
            _blockchainCacheManager.rollbackBlockchainCache();
            _cacheWasMutated = false;
            _blockchainCache = null;
        }

        _databaseConnection.close();
    }
}
