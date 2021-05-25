package com.softwareverde.bitcoin.chain.utxo;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.query.BatchedInsertQuery;
import com.softwareverde.bitcoin.server.database.query.Query;
import com.softwareverde.bitcoin.server.database.query.ValueExtractor;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.utxo.UndoLogDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.utxo.UnspentTransactionOutputManager;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.output.identifier.TransactionOutputIdentifier;
import com.softwareverde.bitcoin.transaction.script.locking.LockingScript;
import com.softwareverde.concurrent.service.GracefulSleepyService;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.MultisetHash;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.row.Row;
import com.softwareverde.database.util.TransactionUtil;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.Tuple;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.NanoTimer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class UtxoCommitmentGenerator extends GracefulSleepyService {
    protected static final String LAST_STAGED_COMMITMENT_BLOCK_HEIGHT_KEY = "last_staged_commitment_block_height";
    protected static final Integer UTXO_COMMITMENT_BLOCK_LAG = UndoLogDatabaseManager.MAX_REORG_DEPTH;

    protected final FullNodeDatabaseManagerFactory _databaseManagerFactory;
    protected final Long _maxByteCountPerFile;
    protected final Long _publishCommitInterval = 10000L;
    protected final String _outputDirectory;

    protected Long _getStagedUtxoCommitmentBlockHeight(final FullNodeDatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT value FROM properties WHERE `key` = ?")
                .setParameter(LAST_STAGED_COMMITMENT_BLOCK_HEIGHT_KEY)
        );
        if (rows.isEmpty()) { return 0L; }

        final Row row = rows.get(0);
        return row.getLong("value");
    }

    protected void _setStagedUtxoCommitmentBlockHeight(final Long blockHeight, final FullNodeDatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();

        databaseConnection.executeSql(
            new Query("INSERT INTO properties (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES (value)")
                .setParameter(LAST_STAGED_COMMITMENT_BLOCK_HEIGHT_KEY)
                .setParameter(blockHeight)
        );
    }

    protected final UtxoCommitmentId _createUtxoCommit(final BlockId blockId, final DatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();

        final Long utxoCommitId = databaseConnection.executeSql(
            new Query("INSERT INTO unspent_transaction_output_commitments (block_id) VALUES (?)")
                .setParameter(blockId)
        );

        return UtxoCommitmentId.wrap(utxoCommitId);
    }

    protected final void _setUtxoCommitHash(final UtxoCommitmentId utxoCommitmentId, final Sha256Hash hash, final DatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();

        databaseConnection.executeSql(
            new Query("UPDATE unspent_transaction_output_commitments SET hash = ? WHERE id = ?")
                .setParameter(hash)
                .setParameter(utxoCommitmentId)
        );
    }

    protected File _createPartialUtxoCommitFile(final UtxoCommitmentId utxoCommitmentId, final MultisetHash multisetHash, final File partialFile, final Integer utxoCount, final Long fileByteCount, final DatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
        final Sha256Hash newFileName = multisetHash.getHash();

        final Sha256Hash hash = multisetHash.getHash();

        databaseConnection.executeSql(
            new Query("INSERT INTO unspent_transaction_output_commitment_files (utxo_commitment_id, hash, utxo_count, byte_count) VALUES (?, ?, ?, ?)")
                .setParameter(utxoCommitmentId)
                .setParameter(hash)
                .setParameter(utxoCount)
                .setParameter(fileByteCount)
        );

        final File newFile = new File(_outputDirectory, newFileName.toString());
        final boolean renameWasSuccessful = partialFile.renameTo(newFile);
        if (! renameWasSuccessful) {
            throw new DatabaseException("Unable to create partial commit file: " + newFile.getAbsolutePath());
        }

        Logger.trace("Partial UTXO Commitment file created: " + newFile + ", " + fileByteCount + " bytes, " + utxoCount + " utxos.");

        return newFile;
    }

    protected UtxoCommitment _publishUtxoCommitment(final BlockId blockId, final FullNodeDatabaseManager databaseManager) throws DatabaseException {
        Logger.debug("Creating UTXO commitment.");
        final NanoTimer nanoTimer = new NanoTimer();
        nanoTimer.start();

        final File outputDirectory = new File(_outputDirectory);
        if ( (! outputDirectory.exists()) || (! outputDirectory.canWrite()) ) {
            throw new DatabaseException("Unable to write UTXO commit to directory: " + _outputDirectory);
        }

        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();

        final int batchSize = Math.min(1024, databaseManager.getMaxQueryBatchSize());
        final int pageSize = (int) (16L * ByteUtil.Unit.Binary.MEBIBYTES);
        final MultisetHash multisetHash = new MultisetHash();
        final File partialFile = new File(outputDirectory, "utxo.dat");

        OutputStream outputStream = null;
        try {
            final UtxoCommitmentId utxoCommitmentId = _createUtxoCommit(blockId, databaseManager);

            final UtxoCommitment utxoCommitment = new UtxoCommitment();
            utxoCommitment._multisetHash = multisetHash;
            utxoCommitment._blockId = blockId;

            int utxoCount = 0;
            long bytesWrittenToStream = 0L;
            outputStream = new BufferedOutputStream(new FileOutputStream(partialFile), pageSize);
            TransactionOutputIdentifier previousTransactionOutputIdentifier = new TransactionOutputIdentifier(Sha256Hash.EMPTY_HASH, 0);

            while (true) {
                final java.util.List<Row> rows;
                {
                    final Sha256Hash transactionHash = previousTransactionOutputIdentifier.getTransactionHash();
                    final Integer outputIndex = previousTransactionOutputIdentifier.getOutputIndex();
                    rows = databaseConnection.query(
                        new Query("SELECT transaction_hash, `index`, block_height, is_coinbase, amount, locking_script FROM staged_unspent_transaction_output_commitment WHERE (transaction_hash > ?) OR (transaction_hash = ? AND `index` > ?) ORDER BY transaction_hash ASC, `index` ASC LIMIT " + batchSize)
                            .setParameter(transactionHash)
                            .setParameter(transactionHash)
                            .setParameter(outputIndex)
                    );
                }
                if (rows.isEmpty()) { break; }

                final int rowCount = rows.size();
                final ByteArray[] serializedUtxoBytes = new ByteArray[rowCount];

                { // Asynchronously deserialize and calculate the MultisetHash.
                    final MutableList<Tuple<Integer, Row>> rowWithIndexes = new MutableList<>(rowCount);
                    {
                        int i = 0;
                        for (final Row row : rows) {
                            final Tuple<Integer, Row> tuple = new Tuple<>(i, row);
                            rowWithIndexes.add(tuple);
                            i += 1;
                        }
                    }

                    final int maxItemsPerBatch;
                    {
                        final Runtime runtime = Runtime.getRuntime();
                        final int cpuCount = runtime.availableProcessors();
                        maxItemsPerBatch = ((rowCount / cpuCount) + 1);
                    }
                    final BatchRunner<Tuple<Integer, Row>> rowBatchRunner = new BatchRunner<>(maxItemsPerBatch, true);
                    rowBatchRunner.run(rowWithIndexes, new BatchRunner.Batch<Tuple<Integer, Row>>() {
                        @Override
                        public void run(final List<Tuple<Integer, Row>> batchItems) {
                            for (final Tuple<Integer, Row> tuple : batchItems) {
                                final int index = tuple.first;
                                final Row row = tuple.second;

                                final Sha256Hash transactionHash = Sha256Hash.wrap(row.getBytes("transaction_hash"));
                                final Integer outputIndex = row.getInteger("index");

                                final Long blockHeight = row.getLong("block_height");
                                final Boolean isCoinbase = row.getBoolean("is_coinbase");
                                final Long amount = row.getLong("amount");
                                final ByteArray lockingScriptBytes = ByteArray.wrap(row.getBytes("locking_script"));

                                final MutableCommittedUnspentTransactionOutput committedUnspentTransactionOutput = new MutableCommittedUnspentTransactionOutput();
                                committedUnspentTransactionOutput.setTransactionHash(transactionHash);
                                committedUnspentTransactionOutput.setIndex(outputIndex);
                                committedUnspentTransactionOutput.setBlockHeight(blockHeight);
                                committedUnspentTransactionOutput.setIsCoinbaseTransaction(isCoinbase);
                                committedUnspentTransactionOutput.setAmount(amount);
                                committedUnspentTransactionOutput.setLockingScript(lockingScriptBytes);

                                final ByteArray byteArray = committedUnspentTransactionOutput.getBytes();

                                multisetHash.addItem(byteArray);
                                serializedUtxoBytes[index] = byteArray;

                                synchronized (utxoCommitment) {
                                    utxoCommitment._blockHeight = Math.max(utxoCommitment._blockHeight, blockHeight);
                                }
                            }
                        }
                    });
                }

                for (final ByteArray byteArray : serializedUtxoBytes) {
                    if (bytesWrittenToStream + byteArray.getByteCount() > _maxByteCountPerFile) {
                        outputStream.flush();
                        outputStream.close();

                        final File newFile = _createPartialUtxoCommitFile(utxoCommitmentId, multisetHash, partialFile, utxoCount, bytesWrittenToStream, databaseManager);
                        utxoCommitment._files.add(newFile);

                        outputStream = new BufferedOutputStream(new FileOutputStream(partialFile), pageSize);
                        bytesWrittenToStream = 0L;
                        utxoCount = 0;
                    }

                    final int byteCount = byteArray.getByteCount();
                    for (int i = 0; i < byteCount; ++i) {
                        final byte b = byteArray.getByte(i);
                        outputStream.write(b);
                    }
                    bytesWrittenToStream += byteCount;
                    utxoCount += 1;
                }

                { // Update the previousTransactionOutputIdentifier for the next loop...
                    final Row row = rows.get(rowCount - 1);
                    final Sha256Hash transactionHash = Sha256Hash.copyOf(row.getBytes("transaction_hash"));
                    final Integer outputIndex = row.getInteger("index");

                    previousTransactionOutputIdentifier = new TransactionOutputIdentifier(transactionHash, outputIndex);
                }
            }

            outputStream.flush();
            outputStream.close();
            outputStream = null;

            if (bytesWrittenToStream > 0) {
                final File newFile = _createPartialUtxoCommitFile(utxoCommitmentId, multisetHash, partialFile, utxoCount, bytesWrittenToStream, databaseManager);
                utxoCommitment._files.add(newFile);
            }

            final Sha256Hash commitHash = multisetHash.getHash();
            _setUtxoCommitHash(utxoCommitmentId, commitHash, databaseManager);

            nanoTimer.stop();
            Logger.trace("Created UTXO Commitment in " + nanoTimer.getMillisecondsElapsed() + "ms.");

            return utxoCommitment;
        }
        catch (final IOException exception) {
            if (outputStream != null) {
                try { outputStream.close(); }
                catch (final Exception ignored) { }
            }

            throw new DatabaseException(exception);
        }
    }

    protected void _updateStagedCommitment(final FullNodeDatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
        final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
        final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();

        final int batchSize = Math.min(1024, databaseManager.getMaxQueryBatchSize());
        final BatchRunner<TransactionOutputIdentifier> identifierBatchRunner = new BatchRunner<>(batchSize);

        long stagedUtxoBlockHeight = _getStagedUtxoCommitmentBlockHeight(databaseManager);
        final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
        if (headBlockId == null) { return; }

        final Long headBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);
        if ((stagedUtxoBlockHeight + UTXO_COMMITMENT_BLOCK_LAG) > headBlockHeight) { return; }

        final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(headBlockId);

        stagedUtxoBlockHeight = (stagedUtxoBlockHeight + 1L);
        BlockId stagedUtxoBlockId = blockHeaderDatabaseManager.getBlockIdAtHeight(blockchainSegmentId, stagedUtxoBlockHeight);
        while (stagedUtxoBlockId != null) {
            if (_shouldAbort()) { break; }

            final NanoTimer nanoTimer = new NanoTimer();
            nanoTimer.start();

            final Block block = blockDatabaseManager.getBlock(stagedUtxoBlockId);
            if (block == null) { break; }

            final Sha256Hash blockHash = block.getHash();
            Logger.debug("Applying " + blockHash + " to staged UTXO commitment.");

            final UnspentTransactionOutputManager.BlockUtxoDiff blockUtxoDiff = UnspentTransactionOutputManager.getBlockUtxoDiff(block);

            final int spentTransactionOutputCount = blockUtxoDiff.spentTransactionOutputIdentifiers.getCount();
            final int transactionOutputCount = blockUtxoDiff.unspentTransactionOutputIdentifiers.getCount();

            final Map<TransactionOutputIdentifier, TransactionOutput> unspentTransactionOutputMap;
            final MutableList<TransactionOutputIdentifier> sortedSpentIdentifiers = new MutableList<>(spentTransactionOutputCount);
            final MutableList<TransactionOutputIdentifier> sortedUnspentIdentifiers = new MutableList<>(transactionOutputCount);
            {
                final TreeMap<TransactionOutputIdentifier, TransactionOutput> sortedUnspentTransactionOutputIdentifierMap = new TreeMap<>();
                for (int i = 0; i < transactionOutputCount; ++i) {
                    final TransactionOutputIdentifier transactionOutputIdentifier = blockUtxoDiff.unspentTransactionOutputIdentifiers.get(i);
                    final TransactionOutput transactionOutput = blockUtxoDiff.unspentTransactionOutputs.get(i);

                    sortedUnspentTransactionOutputIdentifierMap.put(transactionOutputIdentifier, transactionOutput);
                }

                final TreeSet<TransactionOutputIdentifier> sortedSpentIdentifiersHashSet = new TreeSet<>();
                for (final TransactionOutputIdentifier transactionOutputIdentifier : blockUtxoDiff.spentTransactionOutputIdentifiers) {
                    sortedSpentIdentifiersHashSet.add(transactionOutputIdentifier);
                }

                // Exclude the creation of block outputs that are also deleted by this block...
                for (final TransactionOutputIdentifier transactionOutputIdentifier : sortedUnspentTransactionOutputIdentifierMap.keySet()) {
                    final boolean outputIdentifierWasSpentInThisBlock = sortedSpentIdentifiersHashSet.remove(transactionOutputIdentifier);
                    if (! outputIdentifierWasSpentInThisBlock) {
                        sortedUnspentIdentifiers.add(transactionOutputIdentifier);
                    }
                }

                for (final TransactionOutputIdentifier transactionOutputIdentifier : sortedSpentIdentifiersHashSet) {
                    sortedSpentIdentifiers.add(transactionOutputIdentifier);
                }

                unspentTransactionOutputMap = sortedUnspentTransactionOutputIdentifierMap;
            }

            TransactionUtil.startTransaction(databaseConnection);

            final long blockHeight = stagedUtxoBlockHeight;
            identifierBatchRunner.run(sortedUnspentIdentifiers, new BatchRunner.Batch<TransactionOutputIdentifier>() {
                @Override
                public void run(final List<TransactionOutputIdentifier> transactionOutputIdentifiers) throws Exception {
                    final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO staged_unspent_transaction_output_commitment (transaction_hash, `index`, block_height, is_coinbase, amount, locking_script) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE amount = VALUES(amount)");
                    for (final TransactionOutputIdentifier transactionOutputIdentifier : transactionOutputIdentifiers) {
                        final TransactionOutput transactionOutput = unspentTransactionOutputMap.get(transactionOutputIdentifier);

                        final Sha256Hash transactionHash = transactionOutputIdentifier.getTransactionHash();
                        final Integer outputIndex = transactionOutputIdentifier.getOutputIndex();
                        final LockingScript lockingScript = transactionOutput.getLockingScript();
                        final ByteArray lockingScriptBytes = lockingScript.getBytes();

                        final Boolean isCoinbase = (Util.areEqual(blockUtxoDiff.coinbaseTransactionHash, transactionHash));

                        batchedInsertQuery.setParameter(transactionHash);
                        batchedInsertQuery.setParameter(outputIndex);
                        batchedInsertQuery.setParameter(blockHeight);
                        batchedInsertQuery.setParameter(isCoinbase);
                        batchedInsertQuery.setParameter(transactionOutput.getAmount());
                        batchedInsertQuery.setParameter(lockingScriptBytes);
                    }
                    databaseConnection.executeSql(batchedInsertQuery);
                }
            });

            identifierBatchRunner.run(sortedSpentIdentifiers, new BatchRunner.Batch<TransactionOutputIdentifier>() {
                @Override
                public void run(final List<TransactionOutputIdentifier> spentTransactionOutputIdentifiers) throws Exception {
                    databaseConnection.executeSql(
                        new Query("DELETE FROM staged_unspent_transaction_output_commitment WHERE (transaction_hash, `index`) IN (?)")
                            .setExpandedInClauseParameters(spentTransactionOutputIdentifiers, ValueExtractor.TRANSACTION_OUTPUT_IDENTIFIER) // NOTE: DELETE ... WHERE IN <tuple> will not use the table index, so expanded in-clauses are necessary.
                    );
                }
            });

            if ((blockHeight % _publishCommitInterval) == 0) {
                _publishUtxoCommitment(stagedUtxoBlockId, databaseManager);
                // TODO: Delete old commitment.
            }

            _setStagedUtxoCommitmentBlockHeight(stagedUtxoBlockHeight, databaseManager);

            TransactionUtil.commitTransaction(databaseConnection);

            stagedUtxoBlockId = blockHeaderDatabaseManager.getChildBlockId(blockchainSegmentId, stagedUtxoBlockId);
            stagedUtxoBlockHeight += 1L;

            nanoTimer.stop();
            Logger.trace("Applied " + blockHash + " to staged UTXO commitment in " + nanoTimer.getMillisecondsElapsed() + "ms.");
        }
    }

    @Override
    protected void _onStart() { }

    @Override
    protected Boolean _run() {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            _updateStagedCommitment(databaseManager);
        }
        catch (final DatabaseException exception) {
            Logger.debug(exception);
        }

        return false;
    }

    @Override
    protected void _onSleep() { }

    public UtxoCommitmentGenerator(final FullNodeDatabaseManagerFactory databaseManagerFactory, final String outputDirectory) {
        _databaseManagerFactory = databaseManagerFactory;
        _maxByteCountPerFile = (32L * ByteUtil.Unit.Binary.MEBIBYTES);
        _outputDirectory = outputDirectory;
    }
}