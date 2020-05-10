package com.softwareverde.bitcoin.wallet;

import com.softwareverde.security.secp256k1.key.PrivateKey;
import com.softwareverde.security.secp256k1.key.PublicKey;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.ListUtil;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.logging.Logger;
import com.softwareverde.security.util.HashUtil;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.HexUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.bytearray.ByteArrayBuilder;

import java.util.Comparator;

public class SeedPhraseGenerator {
    protected static final Comparator<String> COMPARATOR = new Comparator<String>() {
        @Override
        public int compare(final String string0, final String string1) {
            return string0.compareToIgnoreCase(string1);
        }
    };

    protected final List<String> _seedWords;

    protected String _toSeedPhrase(final ByteArray data) {
        final ByteArray checksum = _getChecksum(data);

        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        byteArrayBuilder.appendBytes(data);
        byteArrayBuilder.appendBytes(checksum.getBytes());

        final ByteArray byteArray = MutableByteArray.wrap(byteArrayBuilder.build());

        final StringBuilder stringBuilder = new StringBuilder();

        int bitIndex = 0;
        int byteArrayBitLength = (byteArray.getByteCount() * 8 / 11) * 11; // only use length aligned to a multiple of 11
        while (bitIndex < byteArrayBitLength) {
            int wordIndex = 0;
            for (int j = 10; (j >= 0) && (bitIndex < byteArrayBitLength); --j) {
                boolean bitIsSet = byteArray.getBit(bitIndex);
                if (bitIsSet) {
                    wordIndex += (1 << j);
                }
                bitIndex += 1;
            }

            final String word = _seedWords.get(wordIndex);
            if (stringBuilder.length() != 0) {
                stringBuilder.append(" ");
            }
            stringBuilder.append(word);
        }

        return stringBuilder.toString();
    }

    protected ByteArray _getChecksum(final ByteArray data) {
        // A checksum is generated by taking the first length / 32 bits of the SHA256 hash of the data
        final int checksumByteLength = (int) Math.ceil(data.getByteCount() / 32.0);
        final int checksumBitLength = (data.getByteCount() * 8) / 32;
        final ByteArray dataHash = HashUtil.sha256(data);
        final MutableByteArray checksum = MutableByteArray.wrap(dataHash.getBytes(0, checksumByteLength));
        for (int i = checksumBitLength; i < (checksumByteLength * 8); ++i) {
            checksum.setBit(i, false);
        }
        return checksum;
    }

    protected ByteArray _fromSeedPhrase(final String expectedSeedPhrase) throws IllegalArgumentException {
        final String[] seedWords = expectedSeedPhrase.split(" ");
        final int byteArrayLength = (int) Math.ceil((seedWords.length * 11.0) / 8);
        final MutableByteArray byteArray = new MutableByteArray(byteArrayLength);

        int runningBitIndex = 0;
        for (final String seedWord : seedWords) {
            final int index = _seedWords.indexOf(seedWord.toLowerCase());
            if (index < 0) {
                throw new IllegalArgumentException("Invalid seed word: " + seedWord);
            }
            final ByteArray seedWordIndexBytes = MutableByteArray.wrap(ByteUtil.integerToBytes(index));
            final int seedWordIndexBitCount = seedWordIndexBytes.getByteCount() * 8;
            for (int i = 10; i >= 0; --i) {
                final boolean seedWordBitIsSet = seedWordIndexBytes.getBit(seedWordIndexBitCount - 1 - i);
                byteArray.setBit(runningBitIndex, seedWordBitIsSet);
                runningBitIndex++;
            }
        }

        final int checksumLength = (int) Math.ceil(byteArrayLength / 33.0);
        final int originalDataLength = byteArrayLength - checksumLength;
        final ByteArray originalData = MutableByteArray.wrap(byteArray.getBytes(0, originalDataLength));
        final byte[] extractedChecksumBytes = byteArray.getBytes(originalDataLength, checksumLength);

        final ByteArray calculatedChecksum = _getChecksum(originalData);
        if (! Util.areEqual(calculatedChecksum.getBytes(), extractedChecksumBytes)) {
            throw new IllegalArgumentException("Invalid seed phrase checksum: expected " + HexUtil.toHexString(extractedChecksumBytes) + " but found " + calculatedChecksum.toString());
        }

        return originalData;
    }

    /**
     * Initializes the SeedPhraseGenerator.
     *  The list of seed words must be already sorted and should contain 2048 unique words.
     */
    public SeedPhraseGenerator(final List<String> seedWords) {
        _seedWords = seedWords.asConst();
    }

    public String toSeedPhrase(final PrivateKey privateKey) {
        return _toSeedPhrase(privateKey);
    }

    public String toSeedPhrase(final PublicKey publicKey) {
        return _toSeedPhrase(publicKey);
    }

    public String toSeedPhrase(final ByteArray data) {
        return _toSeedPhrase(data);
    }

    /**
     * <p>Returns a list of validation errors.</p>
     *
     * <p>If the seed phrase is valid, an empty list is returned.</p>
     * @param expectedSeedPhrase
     * @return
     */
    public List<String> validateSeedPhrase(final String expectedSeedPhrase) {
        final MutableList<String> errors = new MutableList<String>();

        final String[] seedWords = expectedSeedPhrase.split(" ");
        for (final String word : seedWords) {
            final int index = ListUtil.binarySearch(_seedWords, word, COMPARATOR);
            if (index < 0) {
                errors.add("Invalid word: " + word);
            }
        }

        if (errors.isEmpty()) {
            try {
                _fromSeedPhrase(expectedSeedPhrase);
            }
            catch (final Exception exception) {
                errors.add("Unable to parse seed phrase: " + exception.getMessage());
            }
        }

        return errors;
    }

    public Boolean isSeedPhraseValid(final String seedPhrase) {
        try {
            _fromSeedPhrase(seedPhrase);
            return true;
        }
        catch (final Exception exception) {
            return false;
        }
    }

    public ByteArray fromSeedPhrase(final String seedPhrase) {
        try {
            return _fromSeedPhrase(seedPhrase);
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    public List<String> getSeedWords() {
        return _seedWords;
    }
}
