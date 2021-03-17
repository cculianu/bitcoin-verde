package com.softwareverde.bitcoin.server.message.type.dsproof;

import com.softwareverde.bitcoin.transaction.locktime.LockTime;
import com.softwareverde.bitcoin.transaction.locktime.SequenceNumber;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;

public class MutableDoubleSpendProofPreimage extends DoubleSpendProofPreimage {

    public MutableDoubleSpendProofPreimage() { }

    public void setTransactionVersion(final Long transactionVersion) {
        _transactionVersion = transactionVersion;
    }

    public void setSequenceNumber(final SequenceNumber sequenceNumber) {
        _sequenceNumber = sequenceNumber;
    }

    public void setLockTime(final LockTime lockTime) {
        _lockTime = lockTime;
    }

    public void setPreviousOutputsDigest(final Sha256Hash previousOutputsDigest) {
        _previousOutputsDigest = previousOutputsDigest;
    }

    public void setSequenceNumbersDigest(final Sha256Hash sequenceNumbersDigest) {
        _sequenceNumbersDigest = sequenceNumbersDigest;
    }

    public void setTransactionOutputsDigest(final Sha256Hash transactionOutputsDigest) {
        _transactionOutputsDigest = transactionOutputsDigest;
    }

    public void addUnlockingScriptPushData(final ByteArray unlockingScriptPushData) {
        _unlockingScriptPushData.add(unlockingScriptPushData);
    }

    public void clearUnlockingScriptData() {
        _unlockingScriptPushData.clear();
    }
}
