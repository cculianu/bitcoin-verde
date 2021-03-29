package com.softwareverde.bitcoin.server.message.type.dsproof;

import com.softwareverde.bitcoin.server.message.BitcoinProtocolMessageInflater;
import com.softwareverde.bitcoin.server.message.header.BitcoinProtocolMessageHeader;
import com.softwareverde.bitcoin.server.message.type.MessageType;
import com.softwareverde.bitcoin.transaction.dsproof.DoubleSpendProof;
import com.softwareverde.bitcoin.transaction.output.identifier.TransactionOutputIdentifier;
import com.softwareverde.bitcoin.util.bytearray.ByteArrayReader;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.util.bytearray.Endian;

public class DoubleSpendProofMessageInflater extends BitcoinProtocolMessageInflater {
    @Override
    public DoubleSpendProofMessage fromBytes(final byte[] bytes) {
        final ByteArrayReader byteArrayReader = new ByteArrayReader(bytes);

        final BitcoinProtocolMessageHeader protocolMessageHeader = _parseHeader(byteArrayReader, MessageType.DOUBLE_SPEND_PROOF);
        if (protocolMessageHeader == null) { return null; }

        final Sha256Hash previousOutputTransactionHash = Sha256Hash.wrap(byteArrayReader.readBytes(Sha256Hash.BYTE_COUNT, Endian.LITTLE));
        final Integer previousOutputIndex = byteArrayReader.readInteger(4, Endian.LITTLE);
        final TransactionOutputIdentifier transactionOutputIdentifier = new TransactionOutputIdentifier(previousOutputTransactionHash, previousOutputIndex);

        final DoubleSpendProofPreimageInflater doubleSpendProofPreimageInflater = new DoubleSpendProofPreimageInflater();
        final MutableDoubleSpendProofPreimage doubleSpendProofPreimage0 = doubleSpendProofPreimageInflater.fromBytes(byteArrayReader);
        final MutableDoubleSpendProofPreimage doubleSpendProofPreimage1 = doubleSpendProofPreimageInflater.fromBytes(byteArrayReader);

        // Parse the DoubleSpendProofPreimage extra data...
        doubleSpendProofPreimageInflater.parseExtraTransactionOutputsDigests(byteArrayReader, doubleSpendProofPreimage0);
        doubleSpendProofPreimageInflater.parseExtraTransactionOutputsDigests(byteArrayReader, doubleSpendProofPreimage1);

        if (byteArrayReader.didOverflow()) { return null; }

        final DoubleSpendProof doubleSpendProof = new DoubleSpendProof(transactionOutputIdentifier, doubleSpendProofPreimage0, doubleSpendProofPreimage1);
        return new DoubleSpendProofMessage(doubleSpendProof);
    }
}
