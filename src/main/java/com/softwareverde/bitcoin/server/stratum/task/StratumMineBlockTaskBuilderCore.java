package com.softwareverde.bitcoin.server.stratum.task;

import com.softwareverde.bitcoin.block.CanonicalMutableBlock;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.bytearray.FragmentedBytes;
import com.softwareverde.bitcoin.transaction.MutableTransaction;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.TransactionWithFee;
import com.softwareverde.bitcoin.transaction.coinbase.CoinbaseTransaction;
import com.softwareverde.bitcoin.transaction.coinbase.MutableCoinbaseTransaction;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.HexUtil;
import com.softwareverde.util.type.time.SystemTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StratumMineBlockTaskBuilderCore implements ConfigurableStratumMineBlockTaskBuilder {
    final static Object _mutex = new Object();
    private static Long _nextId = 1L;
    protected static Long getNextId() {
        synchronized (_mutex) {
            final Long id = _nextId;
            _nextId += 1;
            return id;
        }
    }

    protected final SystemTime _systemTime = new SystemTime();
    protected final TransactionDeflater _transactionDeflater;

    protected final ConcurrentHashMap<Sha256Hash, TransactionWithFee> _transactionsWithFee = new ConcurrentHashMap<Sha256Hash, TransactionWithFee>();
    protected final CanonicalMutableBlock _prototypeBlock = new CanonicalMutableBlock();
    protected final Integer _totalExtraNonceByteCount;

    protected String _extraNonce1;
    protected String _coinbaseTransactionHead;
    protected String _coinbaseTransactionTail;
    protected Long _blockHeight;

    protected final ReentrantReadWriteLock.ReadLock _prototypeBlockReadLock;
    protected final ReentrantReadWriteLock.WriteLock _prototypeBlockWriteLock;

    protected void _setCoinbaseTransaction(final Transaction coinbaseTransaction) {
        try {
            _prototypeBlockWriteLock.lock();

            final FragmentedBytes coinbaseTransactionParts;
            coinbaseTransactionParts = _transactionDeflater.fragmentTransaction(coinbaseTransaction);

            // NOTE: _coinbaseTransactionHead contains the unlocking script. This script contains two items:
            //  1. The Coinbase Message (ex: "/Mined via Bitcoin-Verde v0.0.1/")
            //  2. The extraNonce (which itself is composed of two components: extraNonce1 and extraNonce2...)
            // extraNonce1 is usually defined by the Mining Pool, not the Miner. The Miner is sent (by the Pool) the number
            // of bytes it should use when generating the extraNonce2 during the Pool's response to the Miner's SUBSCRIBE message.
            // Despite extraNonce just being random data, it still needs to be pushed like regular data within the unlocking script.
            //  Thus, the unlocking script is generated by pushing N bytes (0x00), where N is the byteCount of the extraNonce
            //  (extraNonceByteCount = extraNonce1ByteCount + extraNonce2ByteCount). This results in appropriate operation code
            //  being prepended to the script.  These 0x00 bytes are omitted when stored within _coinbaseTransactionHead,
            //  otherwise, the Miner would appending the extraNonce after the 0x00 bytes instead of replacing them...
            //
            //  Therefore, assuming N is 8, the 2nd part of the unlocking script would originally look something like:
            //
            //      OPCODE  | EXTRA NONCE 1         | EXTRA NONCE 2
            //      -----------------------------------------------------
            //      0x08    | 0x00 0x00 0x00 0x00   | 0x00 0x00 0x00 0x00
            //
            //  Then, stored within _coinbaseTransactionHead (to be sent to the Miner) simply as:
            //      0x08    |                       |
            //

            final Integer headByteCountExcludingExtraNonces = (coinbaseTransactionParts.headBytes.length - _totalExtraNonceByteCount);
            _coinbaseTransactionHead = HexUtil.toHexString(ByteUtil.copyBytes(coinbaseTransactionParts.headBytes, 0, headByteCountExcludingExtraNonces));
            _coinbaseTransactionTail = HexUtil.toHexString(coinbaseTransactionParts.tailBytes);

            _prototypeBlock.replaceTransaction(0, coinbaseTransaction);
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    protected void _initPrototypeBlock() {
        _prototypeBlock.addTransaction(new MutableTransaction());

        // NOTE: Actual nonce and timestamp are updated later within the MineBlockTask...
        _prototypeBlock.setTimestamp(0L);
        _prototypeBlock.setNonce(0L);
    }

    public StratumMineBlockTaskBuilderCore(final Integer totalExtraNonceByteCount, final TransactionDeflater transactionDeflater) {
        _totalExtraNonceByteCount = totalExtraNonceByteCount;
        _transactionDeflater = transactionDeflater;

        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        _prototypeBlockReadLock = readWriteLock.readLock();
        _prototypeBlockWriteLock = readWriteLock.writeLock();

        _initPrototypeBlock();
    }

    @Override
    public void setBlockVersion(final Long blockVersion) {
        try {
            _prototypeBlockWriteLock.lock();

            _prototypeBlock.setVersion(blockVersion);
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public void setPreviousBlockHash(final Sha256Hash previousBlockHash) {
        try {
            _prototypeBlockWriteLock.lock();

            _prototypeBlock.setPreviousBlockHash(previousBlockHash);
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public void setExtraNonce(final ByteArray extraNonce) {
        _extraNonce1 = HexUtil.toHexString(extraNonce.getBytes());
    }

    @Override
    public void setDifficulty(final Difficulty difficulty) {
        try {
            _prototypeBlockWriteLock.lock();

            _prototypeBlock.setDifficulty(difficulty);
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public void addTransaction(final TransactionWithFee transactionWithFee) {
        try {
            _prototypeBlockWriteLock.lock();

            final Transaction transaction = transactionWithFee.transaction;
            final Long transactionFee = transactionWithFee.transactionFee;

            _prototypeBlock.addTransaction(transaction);
            _transactionsWithFee.put(transaction.getHash(), transactionWithFee);

            final CoinbaseTransaction coinbaseTransaction = _prototypeBlock.getCoinbaseTransaction();
            final MutableCoinbaseTransaction mutableCoinbaseTransaction = new MutableCoinbaseTransaction(coinbaseTransaction);
            final Long currentBlockReward = coinbaseTransaction.getBlockReward();
            mutableCoinbaseTransaction.setBlockReward(currentBlockReward + transactionFee);

            _setCoinbaseTransaction(mutableCoinbaseTransaction);
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public CoinbaseTransaction getCoinbaseTransaction() {
        return _prototypeBlock.getCoinbaseTransaction();
    }

    @Override
    public void removeTransaction(final Sha256Hash transactionHash) {
        try {
            _prototypeBlockWriteLock.lock();

            _prototypeBlock.removeTransaction(transactionHash);

            final TransactionWithFee transactionWithFee = _transactionsWithFee.get(transactionHash);
            if (transactionWithFee == null) {
                Logger.warn("Unable to remove transaction from prototype block: " + transactionHash);
                return;
            }

            final Long transactionFee = transactionWithFee.transactionFee;

            final CoinbaseTransaction coinbaseTransaction = _prototypeBlock.getCoinbaseTransaction();
            final MutableCoinbaseTransaction mutableCoinbaseTransaction = new MutableCoinbaseTransaction(coinbaseTransaction);
            final Long currentBlockReward = coinbaseTransaction.getBlockReward();
            mutableCoinbaseTransaction.setBlockReward(currentBlockReward - transactionFee);

            _setCoinbaseTransaction(mutableCoinbaseTransaction);

        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public void clearTransactions() {
        try {
            _prototypeBlockWriteLock.lock();

            final Transaction coinbaseTransaction = _prototypeBlock.getCoinbaseTransaction();
            _prototypeBlock.clearTransactions();
            _prototypeBlock.addTransaction(coinbaseTransaction);
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public void setCoinbaseTransaction(final Transaction coinbaseTransaction) {
        _setCoinbaseTransaction(coinbaseTransaction);
    }

    @Override
    public StratumMineBlockTask buildMineBlockTask() {
        try {
            _prototypeBlockReadLock.lock();

            final ByteArray id = MutableByteArray.wrap(ByteUtil.integerToBytes(StratumMineBlockTaskBuilderCore.getNextId()));
            return new StratumMineBlockTask(id, _prototypeBlock, _coinbaseTransactionHead, _coinbaseTransactionTail, _extraNonce1);
        }
        finally {
            _prototypeBlockReadLock.unlock();
        }
    }

    @Override
    public void setBlockHeight(final Long blockHeight) {
        try {
            _prototypeBlockWriteLock.lock();

            _blockHeight = blockHeight;
        }
        finally {
            _prototypeBlockWriteLock.unlock();
        }
    }

    @Override
    public Long getBlockHeight() {
        try {
            _prototypeBlockReadLock.lock();

            return _blockHeight;
        }
        finally {
            _prototypeBlockReadLock.unlock();
        }
    }
}
