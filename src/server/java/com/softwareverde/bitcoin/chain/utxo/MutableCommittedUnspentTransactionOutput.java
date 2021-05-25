package com.softwareverde.bitcoin.chain.utxo;

import com.softwareverde.bitcoin.transaction.output.MutableUnspentTransactionOutput;
import com.softwareverde.bitcoin.transaction.output.UnspentTransactionOutput;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.util.bytearray.ByteArrayBuilder;
import com.softwareverde.util.bytearray.Endian;

public class MutableCommittedUnspentTransactionOutput extends MutableUnspentTransactionOutput implements CommittedUnspentTransactionOutput {
    protected Sha256Hash _transactionHash;
    protected Boolean _transactionIsCoinbase = false;

    public MutableCommittedUnspentTransactionOutput() { }

    public MutableCommittedUnspentTransactionOutput(final Sha256Hash transactionHash, final UnspentTransactionOutput unspentTransactionOutput) {
        super(unspentTransactionOutput);
        _transactionHash = transactionHash;
    }

    public MutableCommittedUnspentTransactionOutput(final CommittedUnspentTransactionOutput unspentTransactionOutput) {
        super(unspentTransactionOutput);
        _transactionHash = unspentTransactionOutput.getTransactionHash();
        _transactionIsCoinbase = unspentTransactionOutput.isCoinbaseTransaction();
    }

    public void setTransactionHash(final Sha256Hash transactionHash) {
        _transactionHash = transactionHash;
    }

    public void setIsCoinbaseTransaction(final Boolean transactionIsCoinbase) {
        _transactionIsCoinbase = transactionIsCoinbase;
    }

    @Override
    public Boolean isCoinbaseTransaction() {
        return _transactionIsCoinbase;
    }

    @Override
    public Sha256Hash getTransactionHash() {
        return _transactionHash;
    }

    @Override
    public Integer getByteCount() {
        return (CommittedUnspentTransactionOutputInflater.NON_LOCKING_SCRIPT_BYTE_COUNT + _lockingScript.getByteCount());
    }

    @Override
    public ByteArray getBytes() {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();

        final MutableByteArray blockHeightAndIsCoinbaseBytes = MutableByteArray.wrap(ByteUtil.integerToBytes(_blockHeight));
        if (_transactionIsCoinbase) {
            blockHeightAndIsCoinbaseBytes.setBit(CommittedUnspentTransactionOutput.IS_COINBASE_FLAG_BIT_INDEX, true);
        }

        byteArrayBuilder.appendBytes(_transactionHash, Endian.LITTLE);
        byteArrayBuilder.appendBytes(ByteUtil.integerToBytes(_index), Endian.LITTLE);
        byteArrayBuilder.appendBytes(blockHeightAndIsCoinbaseBytes, Endian.LITTLE);
        byteArrayBuilder.appendBytes(ByteUtil.longToBytes(_amount), Endian.LITTLE);
        byteArrayBuilder.appendBytes(ByteUtil.integerToBytes(_lockingScript.getByteCount()), Endian.LITTLE);
        byteArrayBuilder.appendBytes(_lockingScript.getBytes());

        return byteArrayBuilder;
    }
}