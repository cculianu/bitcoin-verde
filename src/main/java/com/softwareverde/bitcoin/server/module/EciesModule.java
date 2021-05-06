package com.softwareverde.bitcoin.server.module;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.ecies.EciesDecrypt;
import com.softwareverde.cryptography.secp256k1.ecies.EciesEncrypt;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.cryptography.secp256k1.key.PublicKey;
import com.softwareverde.cryptography.util.HashUtil;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.IoUtil;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class EciesModule implements AutoCloseable {
    public enum Action {
        ENCRYPT, DECRYPT
    }

    protected final BufferedReader _bufferedReader;

    protected String _readLine() {
        try {
            return _bufferedReader.readLine();
        }
        catch (final Exception exception) {
            Logger.debug(exception);
            return null;
        }
    }

    protected ByteArray _toBytes(final char[] charArray) {
        final ByteBuffer byteBuffer = StandardCharsets.UTF_16.encode(CharBuffer.wrap(charArray));
        final byte[] bytes = byteBuffer.array();
        return ByteArray.wrap(bytes);
    }

    protected PrivateKey _derivePrivateKey(final char[] charArray) {
        final ByteArray passwordBytes = _toBytes(charArray);
        final Sha256Hash passwordHash = HashUtil.doubleSha256(passwordBytes);
        return PrivateKey.fromBytes(passwordHash);
    }

    protected void _runEncrypt() {
        System.out.print("Enter the file to encrypt: ");
        final String inputFileName = _readLine();

        final ByteArray fileContents = ByteArray.wrap(IoUtil.getFileContents(inputFileName));
        if ( (fileContents == null) || fileContents.isEmpty() ) {
            System.err.println("Unable to read contents of file.");
            System.exit(1);
            return;
        }

        System.out.print("Enter the destination of the encrypted file: ");
        final String outputFileName = _readLine();

        final File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            System.err.println("Destination exists, aborting.");
            System.exit(1);
            return;
        }

        final Console console = System.console();
        System.out.print("Enter the password to encrypt: ");
        final char[] passwordBytes = console.readPassword();
        System.out.print("Repeat the password: ");
        final char[] passwordBytes2 = console.readPassword();
        if (! Arrays.equals(passwordBytes, passwordBytes2)) {
            System.err.println("Password mismatch.");
            System.exit(1);
            return;
        }

        final PrivateKey privateKey = _derivePrivateKey(passwordBytes);
        final PublicKey publicKey = privateKey.getPublicKey();

        final EciesEncrypt eciesEncrypt = new EciesEncrypt(publicKey);
        final ByteArray byteArray = eciesEncrypt.encrypt(fileContents);

        IoUtil.putFileContents(outputFile, byteArray);
        System.out.println("Encrypted contents saved to: " + outputFileName);
    }

    protected void _runDecrypt() {
        System.out.print("Enter the file to decrypt: ");
        final String inputFileName = _readLine();

        final ByteArray fileContents = ByteArray.wrap(IoUtil.getFileContents(inputFileName));
        if ( (fileContents == null) || fileContents.isEmpty() ) {
            System.err.println("Unable to read contents of file.");
            System.exit(1);
            return;
        }

        System.out.print("Enter the destination of the decrypted file: ");
        final String outputFileName = _readLine();

        final File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            System.err.println("Destination exists, aborting.");
            System.exit(1);
            return;
        }

        System.out.print("Enter the password to decrypt: ");
        final Console console = System.console();
        final char[] passwordBytes = console.readPassword();

        final PrivateKey privateKey = _derivePrivateKey(passwordBytes);

        final EciesDecrypt eciesDecrypt = new EciesDecrypt(privateKey);
        final ByteArray byteArray = eciesDecrypt.decrypt(fileContents);

        IoUtil.putFileContents(outputFile, byteArray);
        System.out.println("Encrypted contents saved to: " + outputFileName);
    }

    public EciesModule() {
        final InputStreamReader inputStreamReader = new InputStreamReader(System.in);
        _bufferedReader = new BufferedReader(inputStreamReader);
    }

    public void run(final Action action) {
        if (action == Action.ENCRYPT) {
            _runEncrypt();
        }
        else if (action == Action.DECRYPT) {
            _runDecrypt();
        }
        else {
            System.err.println("Unknown action: " + action);
            System.exit(1);
        }
    }

    @Override
    public void close() {
        try {
            _bufferedReader.close();
        }
        catch (final Exception exception) {
            Logger.trace(exception);
        }
    }
}
