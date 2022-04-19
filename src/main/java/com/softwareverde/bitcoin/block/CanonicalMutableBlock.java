package com.softwareverde.bitcoin.block;

import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;

import java.util.Comparator;

public class CanonicalMutableBlock extends MutableBlock {
    public static final Comparator<Transaction> LEXICAL_TRANSACTION_ORDERING = new Comparator<Transaction>() {
        @Override
        public int compare(final Transaction transaction0, final Transaction transaction1) {
            final Sha256Hash hash0 = transaction0.getHash();
            final Sha256Hash hash1 = transaction1.getHash();

            return Sha256Hash.COMPARATOR.compare(hash0, hash1);
        }
    };

    protected static List<Transaction> sortTransactions(final Transaction coinbaseTransaction, final List<Transaction> transactions) {
        if ( (transactions == null) || transactions.isEmpty() ) {
            return new ImmutableList<>(coinbaseTransaction);
        }

        final int nonCoinbaseTransactionCount = transactions.getCount();
        final MutableList<Transaction> _transactions = new MutableList<>(nonCoinbaseTransactionCount + 1);

        for (int i = 0; i < nonCoinbaseTransactionCount; ++i) {
            final Transaction transaction = transactions.get(i);
            final Transaction constTransaction = transaction.asConst();
            _transactions.add(constTransaction);
        }
        _transactions.sort(LEXICAL_TRANSACTION_ORDERING);

        _transactions.add(0, coinbaseTransaction); // Adds the coinbase to the front...

        return _transactions;
    }

    protected static List<Transaction> sortTransactionsWithCoinbase(final List<Transaction> transactions) {
        final int transactionCount = transactions.getCount();
        final MutableList<Transaction> _transactions = new MutableList<>(transactionCount);
        if (transactions.isEmpty()) { return _transactions; }

        for (int i = 1; i < transactionCount; ++i) { // Excludes coinbase...
            final Transaction transaction = transactions.get(i);
            final Transaction constTransaction = transaction.asConst();
            _transactions.add(constTransaction);
        }
        _transactions.sort(LEXICAL_TRANSACTION_ORDERING);

        final Transaction coinbaseTransaction = transactions.get(0);
        _transactions.add(0, coinbaseTransaction); // Adds the coinbase to the front...
        return _transactions;
    }

    // Adds newTransaction to the Block's set of transactions in lexical order.
    //  Rebuilds the Block's MerkleTree.
    //  If newTransaction is a duplicate then it is not added to the Block, but the MerkleTree is still rebuilt.
    protected void _addTransaction(final Transaction newTransaction) {
        final Transaction newConstTransaction = newTransaction.asConst();

        _merkleTree.clear();

        if (! _transactions.isEmpty()) {
            final Transaction coinbaseTransaction = _transactions.get(0);
            _merkleTree.addItem(coinbaseTransaction);
        }

        boolean wasAdded = false;
        for (int i = 1; i < _transactions.getCount(); ++i) { // Exclude coinbase...
            final Transaction existingTransaction = _transactions.get(i);

            if (! wasAdded) {
                final int compareValue = LEXICAL_TRANSACTION_ORDERING.compare(existingTransaction, newConstTransaction);
                if (compareValue == 0) { wasAdded = true; } // Transaction is a duplicate; wasAdded is set to true to prevent newTransaction from being added again...
                else {
                    final boolean newTransactionComesBeforeExistingTransaction = (compareValue > 0);
                    if (newTransactionComesBeforeExistingTransaction) {
                        _transactions.add(i, newConstTransaction); // Insert newTransaction before existingTransaction...
                        _merkleTree.addItem(newConstTransaction);

                        wasAdded = true;
                        continue; // Continue rebuilding the MerkleTree...
                    }
                }
            }

            _merkleTree.addItem(existingTransaction);
        }

        if (! wasAdded) { // Add newTransaction at the end of the Block or as its Coinbase...
            _transactions.add(newConstTransaction);
            _merkleTree.addItem(newConstTransaction);
        }
    }

    public CanonicalMutableBlock() { }

    public CanonicalMutableBlock(final BlockHeader blockHeader) {
        super(blockHeader);
    }

    public CanonicalMutableBlock(final BlockHeader blockHeader, final Transaction coinbaseTransaction, final List<Transaction> transactions) {
        super(blockHeader, CanonicalMutableBlock.sortTransactions(coinbaseTransaction, transactions));
    }

    public CanonicalMutableBlock(final Block block) {
        super(block, CanonicalMutableBlock.sortTransactionsWithCoinbase(block.getTransactions()));
    }

    @Override
    public void addTransaction(final Transaction newTransaction) {
        _addTransaction(newTransaction);
        _invalidateCachedProperties();
    }

    @Override
    public void replaceTransaction(final Integer index, final Transaction transaction) {
        if (index == 0) { // Traditionally replace the coinbase...
            super.replaceTransaction(0, transaction);
            return;
        }

        _transactions.remove(index);
        _addTransaction(transaction); // Also rebuilds the MerkleTree...
        _invalidateCachedProperties();
    }

    @Override
    public void removeTransaction(final Sha256Hash transactionHashToRemove) {
        super.removeTransaction(transactionHashToRemove);
    }

    public void setTransactions(final Transaction coinbaseTransaction, final List<Transaction> transactions) {
        _transactions.clear();
        _merkleTree.clear();

        if (transactions == null) { return; }

        final List<Transaction> sortedTransactions = CanonicalMutableBlock.sortTransactions(coinbaseTransaction, transactions);
        _transactions.addAll(sortedTransactions);
        for (final Transaction transaction : sortedTransactions) {
            _merkleTree.addItem(transaction);
        }

        _invalidateCachedProperties();
    }
}
