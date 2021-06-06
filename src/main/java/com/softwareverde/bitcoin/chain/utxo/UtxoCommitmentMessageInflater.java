package com.softwareverde.bitcoin.chain.utxo;

import com.softwareverde.bitcoin.server.message.BitcoinProtocolMessageInflater;
import com.softwareverde.bitcoin.server.message.header.BitcoinProtocolMessageHeader;
import com.softwareverde.bitcoin.server.message.type.MessageType;
import com.softwareverde.bitcoin.util.bytearray.ByteArrayReader;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.cryptography.secp256k1.key.PublicKey;

public class UtxoCommitmentMessageInflater extends BitcoinProtocolMessageInflater {

    public UtxoCommitmentMessageInflater() { }

    @Override
    public UtxoCommitmentMessage fromBytes(final byte[] bytes) {
        final UtxoCommitmentMessage utxoCommitmentMessage = new UtxoCommitmentMessage();
        final ByteArrayReader byteArrayReader = new ByteArrayReader(bytes);

        final BitcoinProtocolMessageHeader protocolMessageHeader = _parseHeader(byteArrayReader, MessageType.UTXO_COMMITMENT);
        if (protocolMessageHeader == null) { return null; }

        final PublicKey multisetPublicKey = PublicKey.fromBytes(byteArrayReader.readBytes(PublicKey.COMPRESSED_BYTE_COUNT));
        utxoCommitmentMessage.setMultisetPublicKey(multisetPublicKey);

        final Long byteCount = byteArrayReader.readVariableLengthInteger();
        if (byteCount > UtxoCommitmentMessage.MAX_BUCKET_BYTE_COUNT) { return null; }

        final ByteArray utxoCommitmentBytes = ByteArray.wrap(byteArrayReader.readBytes(byteCount.intValue()));
        utxoCommitmentMessage.setUtxoCommitmentBytes(utxoCommitmentBytes);

        if (byteArrayReader.didOverflow()) { return null; }

        return utxoCommitmentMessage;
    }
}
