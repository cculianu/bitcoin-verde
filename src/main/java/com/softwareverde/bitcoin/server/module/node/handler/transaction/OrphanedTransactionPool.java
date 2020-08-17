package com.softwareverde.bitcoin.server.module.node.handler.transaction;

import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.FullNodeTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.output.UnconfirmedTransactionOutputDatabaseManager;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.output.UnconfirmedTransactionOutputId;
import com.softwareverde.bitcoin.transaction.output.identifier.TransactionOutputIdentifier;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.ListUtil;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.logging.Logger;
import com.softwareverde.security.hash.sha256.Sha256Hash;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class OrphanedTransactionPool {
    public interface Callback {
        void newTransactionsAvailable(Set<Transaction> transactions);
    }

    public static final Integer MAX_ORPHANED_TRANSACTION_COUNT = (128 * 1024);

    protected final HashMap<TransactionOutputIdentifier, HashSet<Transaction>> _orphanedTransactions = new HashMap<TransactionOutputIdentifier, HashSet<Transaction>>(MAX_ORPHANED_TRANSACTION_COUNT);
    protected final HashSet<Transaction> _orphanedTransactionSet = new HashSet<Transaction>();
    protected final LinkedList<Transaction> _orphanedTransactionsByAge = new LinkedList<Transaction>();
    protected final Callback _callback;

    protected void _removeOrphanedTransaction(final Transaction transaction) {
        _orphanedTransactionSet.remove(transaction);

        for (final TransactionInput transactionInput : transaction.getTransactionInputs()) {
            final TransactionOutputIdentifier transactionOutputIdentifier = TransactionOutputIdentifier.fromTransactionInput(transactionInput);
            final HashSet<Transaction> queuedTransactions = _orphanedTransactions.get(transactionOutputIdentifier);
            if (queuedTransactions == null) { continue; }

            queuedTransactions.remove(transaction);

            if (queuedTransactions.isEmpty()) {
                _orphanedTransactions.remove(transactionOutputIdentifier);
            }
        }

        Logger.debug("Purging old orphaned Transaction: " + transaction.getHash() + " (" + _orphanedTransactionSet.size() + " / " + MAX_ORPHANED_TRANSACTION_COUNT + ")");
    }

    protected void _purgeOldTransactions() {
        final int itemsToRemoveCount = (MAX_ORPHANED_TRANSACTION_COUNT / 2);
        for (int i = 0; i < itemsToRemoveCount; ++i) {
            if (_orphanedTransactionsByAge.isEmpty()) { break; }

            final Transaction transaction = _orphanedTransactionsByAge.removeFirst();
            if (_orphanedTransactionSet.contains(transaction)) {
                _removeOrphanedTransaction(transaction);
            }
        }
    }

    protected Boolean _transactionOutputExists(final TransactionOutputIdentifier transactionOutputIdentifier, final FullNodeDatabaseManager databaseManager) throws DatabaseException {
        final UnconfirmedTransactionOutputDatabaseManager unconfirmedTransactionOutputDatabaseManager = databaseManager.getUnconfirmedTransactionOutputDatabaseManager();
        final UnconfirmedTransactionOutputId unconfirmedTransactionOutputId = unconfirmedTransactionOutputDatabaseManager.getUnconfirmedTransactionOutputId(transactionOutputIdentifier);
        if (unconfirmedTransactionOutputId != null) { return true; }

        final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
        final TransactionOutput transactionOutput = transactionDatabaseManager.getTransactionOutput(transactionOutputIdentifier);
        return (transactionOutput != null);
    }

    protected Set<Transaction> _onValidTransactionsProcessed(final List<Transaction> transactions) {
        final HashSet<Transaction> eligibleTransactions = new HashSet<Transaction>();

        for (final Transaction transaction : transactions) {
            final Sha256Hash transactionHash = transaction.getHash();

            for (final TransactionOutput transactionOutput : transaction.getTransactionOutputs()) {
                final Integer transactionOutputIndex = transactionOutput.getIndex();
                final TransactionOutputIdentifier transactionOutputIdentifier = new TransactionOutputIdentifier(transactionHash, transactionOutputIndex);
                final HashSet<Transaction> queuedTransactions = _orphanedTransactions.remove(transactionOutputIdentifier);
                if (queuedTransactions == null) { continue; }

                eligibleTransactions.addAll(queuedTransactions);

                Logger.debug("Promoting orphaned Transaction: " + transaction.getHash());
            }

            if (_orphanedTransactionSet.contains(transaction)) {
                _removeOrphanedTransaction(transaction);
            }
        }

        return eligibleTransactions;
    }

    public OrphanedTransactionPool(final Callback callback) {
        _callback = callback;
    }

    /**
     * Adds a Transaction that is unable to be processed due to missing dependent Transactions to the cache.
     */
    public synchronized void add(final Transaction transaction, final FullNodeDatabaseManager databaseManager) throws DatabaseException {
        final boolean transactionIsUnique = _orphanedTransactionSet.add(transaction);
        if (! transactionIsUnique) { return; }

        Logger.debug("Queuing orphaned Transaction: " + transaction.getHash() + " (" + _orphanedTransactionSet.size() + " / " + MAX_ORPHANED_TRANSACTION_COUNT + ")");

        _orphanedTransactionsByAge.addLast(transaction);
        if (_orphanedTransactionSet.size() > MAX_ORPHANED_TRANSACTION_COUNT) {
            _purgeOldTransactions();
        }

        for (final TransactionInput transactionInput : transaction.getTransactionInputs()) {
            final TransactionOutputIdentifier transactionOutputIdentifier = TransactionOutputIdentifier.fromTransactionInput(transactionInput);
            final Boolean transactionOutputBeingSpentExists = _transactionOutputExists(transactionOutputIdentifier, databaseManager);
            if (! transactionOutputBeingSpentExists) {
                if (! _orphanedTransactions.containsKey(transactionOutputIdentifier)) {
                    _orphanedTransactions.put(transactionOutputIdentifier, new HashSet<Transaction>());
                }

                final HashSet<Transaction> queuedTransactions = _orphanedTransactions.get(transactionOutputIdentifier);
                queuedTransactions.add(transaction);
            }
        }
    }

    /**
     * Informs the OrphanedTransactionCache that a new valid Transaction was processed.
     *  Returns a set of Transactions that are now ready for processing with the addition of the new Transaction.
     */
    public void onValidTransactionProcessed(final Transaction transaction) {
        final Set<Transaction> eligibleTransactions;
        synchronized (this) {
            eligibleTransactions = _onValidTransactionsProcessed(ListUtil.newMutableList(transaction));
        }

        if (! eligibleTransactions.isEmpty()) {
            _callback.newTransactionsAvailable(eligibleTransactions);
        }
    }

    public void onValidTransactionsProcessed(final List<Transaction> transactions) {
        final Set<Transaction> eligibleTransactions;
        synchronized (this) {
            eligibleTransactions = _onValidTransactionsProcessed(transactions);
        }

        if (! eligibleTransactions.isEmpty()) {
            _callback.newTransactionsAvailable(eligibleTransactions);
        }
    }
}
