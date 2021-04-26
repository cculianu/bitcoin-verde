package com.softwareverde.bitcoin.server.module;

import com.softwareverde.bitcoin.server.configuration.Configuration;
import com.softwareverde.bitcoin.server.configuration.ConfigurationPropertiesExporter;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.sun.management.OperatingSystemMXBean;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

public class ConfigurationModule {
    static final long KIB_BYTES = 1024;
    static final long MIB_BYTES = 1024 * KIB_BYTES;
    static final long GIB_BYTES = 1024 * MIB_BYTES;

    static final long RECOMMENDED_MEMORY = 6 * GIB_BYTES;

    static final long MAX_PEER_COUNT_LOWER_BOUND = 24;
    static final long MAX_PEER_COUNT_UPPER_BOUND = 128;
    static final long MIN_PEER_COUNT_LOWER_BOUND = 8;
    static final long MIN_PEER_COUNT_UPPER_BOUND = 32;
    static final long MIN_PEER_COUNT_DIVISOR = 4;
    static final long BYTES_PER_PEER = 85 * MIB_BYTES;

    static final long MAX_UTXO_CACHE_BYTE_COUNT_LOWER_BOUND = 1 * GIB_BYTES;
    static final long MAX_UTXO_CACHE_BYTE_COUNT_UPPER_BOUND = 2 * GIB_BYTES;

    static final long MAX_DATABASE_MEMORY_BYTE_COUNT_LOWER_BOUND = 2 * GIB_BYTES;

    final static float PEER_COUNT_EXCESS_PERCENTAGE = 0.25f;
    final static float MAX_UTXO_CACHE_BYTE_COUNT_EXCESS_PERCENTAGE = 0.25f;
    final static float MAX_DATABASE_MEMORY_BYTE_COUNT_EXCESS_PERCENTAGE = 1f - (PEER_COUNT_EXCESS_PERCENTAGE + MAX_UTXO_CACHE_BYTE_COUNT_EXCESS_PERCENTAGE);

    final static int MAX_THREAD_COUNT_LOWER_BOUND = 2;

    final String _configurationFilename;
    final Map<String, String> _userInputMap = new HashMap<>();
    final BufferedReader _bufferedReader = new BufferedReader(new InputStreamReader(System.in));

    public ConfigurationModule(final String configurationFilename) {
        _configurationFilename = configurationFilename;
    }

    public void run() {
        System.out.println("Starting node configuration.");

        try {
            System.out.println("Edit existing configuration? [y/n]");

            final String editExistingConfiguration = _bufferedReader.readLine();
            final Configuration configuration;
            if (editExistingConfiguration.equals("y")) {
                configuration = new Configuration(new File(_configurationFilename));
            }
            else {
                configuration = new Configuration();
            }

            System.out.println("\nPlease specify the following parameters. Select default parameters by returning an empty value.");

            _configureMemoryAllocation();
            _configureMaxThreadCount();
            _configureIndexBlocks();

            System.out.printf("[%s]%n", ConfigurationPropertiesExporter.BAN_FILTER_IS_ENABLED);
            System.out.println("If set to zero or false, then nodes will not be banned under any circumstances. Additionally, any previously banned nodes will be unbanned while disabled.");
            _readUserInput(ConfigurationPropertiesExporter.BAN_FILTER_IS_ENABLED, true);

            // TODO: not needed?
//            System.out.printf("[%s]%n", ConfigurationPropertiesExporter.MAX_MESSAGES_PER_SECOND);
//            System.out.println("May be used to prevent flooding the node; requests exceeding this throttle setting are queued. 250 msg/s supports a little over 32MB blocks.\n");
//            _readUserInput(ConfigurationPropertiesExporter.MAX_MESSAGES_PER_SECOND);

            _bufferedReader.close();

            ConfigurationPropertiesExporter.exportConfiguration(configuration, _configurationFilename, _userInputMap);
        }
        catch (final Exception exception) {
            System.out.println(exception.toString());
            exception.printStackTrace();
            BitcoinUtil.exitFailure();
        }

        System.out.println("Node configuration is complete.");
    }

    private void _readUserInput(final String propertyKey) throws Exception {
        _readUserInput(propertyKey, false);
    }

    private void _readUserInput(final String propertyKey, final boolean isBoolean) throws Exception {
        if (isBoolean) {
            System.out.println("Enter value (1 or 0):");
        }
        else {
            System.out.println("Enter value:");
        }

        final String userInput = _bufferedReader.readLine();
        _userInputMap.put(propertyKey, userInput);
    }

    private void _configureMaxThreadCount() throws Exception {
        System.out.printf("[%s]%n", ConfigurationPropertiesExporter.MAX_THREAD_COUNT);
        System.out.println("The max number of threads used to validate a block. Currently, the server will create max(maxPeerCount * 8, 256) threads for network communication; in the future this property will likely claim this label.");
        System.out.printf("Default value: %d Recommended: %d%n", MAX_THREAD_COUNT_LOWER_BOUND, Runtime.getRuntime().availableProcessors());

        final String userInput = _bufferedReader.readLine();
        if (userInput == null || userInput.isEmpty()) {
            _userInputMap.put(ConfigurationPropertiesExporter.MAX_THREAD_COUNT, String.valueOf(MAX_THREAD_COUNT_LOWER_BOUND));
            return;
        }

        final int maxThreadCount = Integer.parseInt(userInput);
        if (maxThreadCount < MAX_THREAD_COUNT_LOWER_BOUND) {
            System.out.printf("Setting to default value: %d%n", MAX_THREAD_COUNT_LOWER_BOUND);
            _userInputMap.put(ConfigurationPropertiesExporter.MAX_THREAD_COUNT, String.valueOf(MAX_THREAD_COUNT_LOWER_BOUND));
            return;
        }

        _userInputMap.put(ConfigurationPropertiesExporter.MAX_THREAD_COUNT, userInput);
    }

    private void _configureIndexBlocks() throws Exception {
        final long totalSpace = new File("/").getTotalSpace();

        if (totalSpace >= 500 * GIB_BYTES) {
            _userInputMap.put(ConfigurationPropertiesExporter.INDEXING_MODE_IS_ENABLED, "1");
            return;
        }

        System.out.printf("[%s]%n", ConfigurationPropertiesExporter.INDEXING_MODE_IS_ENABLED);
        System.out.printf("WARNING: 500GB+ available free space is recommended for indexing. Total disk space on this device: %dGB.%n", totalSpace / GIB_BYTES);
        _readUserInput(ConfigurationPropertiesExporter.INDEXING_MODE_IS_ENABLED, true);
    }

    private void _configureMemoryAllocation() throws Exception {
        // Need to get user's total system memory (to display) and then the amount of system memory that they want to use
        // Values at or below 6 GB use defaults.
        // Values above 6 GB get scaled accordingly, based on the excess memory available after subtracting the total default memory allocation
        //        [MEMORY]
        //        Z=(maxUtxoCacheByteCount.upper - maxUtxoCacheByteCount.lower) / maxUtxoCacheByteCount.scalePercentage
        //        X=maxPeerCount.scalePercentage+maxMemoryByteCount.scalePercentage

                // if (excess > Z) {maxPeerCountMemoryAllocationScalePercentage = maxPeerCount.scalePercentage / X}
                // else {maxPeerCountMemoryAllocationScalePercentage = maxPeerCount.scalePercentage}

        //        bitcoin.minPeerCount = 8        - 8 / 32 (maxPeerCount / 4, min 8)
        //        bitcoin.maxPeerCount = 24       - 24 / 128 [85MB per peer] (25% of excess, scalePercentage/X excess once excess is past Z)
        //        bitcoin.maxUtxoCacheByteCount = 1073741824  - 1073741824 / 2147483648 (25% excess over 6GB)
        //        bitcoin.database.maxMemoryByteCount = 2147483648    - 2147483648 / unbounded (50% of excess, all excess once excess is past Z)

        final java.lang.management.OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        final OperatingSystemMXBean operatingSystem = ( (operatingSystemMXBean instanceof OperatingSystemMXBean) ? (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean() : null );
        final long maximumMemory = operatingSystem.getTotalPhysicalMemorySize();

        System.out.printf("Specify the maximum amount of memory to be used by the application. Default/Recommended: %dG%n", RECOMMENDED_MEMORY / GIB_BYTES);
        System.out.println("Total available memory: " + (maximumMemory / GIB_BYTES) + "G");

        final long allocatedMemory = Long.min(maximumMemory, _readMemoryValue(RECOMMENDED_MEMORY));
        if (allocatedMemory <= RECOMMENDED_MEMORY) {
            _userInputMap.put("bitcoin." + ConfigurationPropertiesExporter.MAX_MEMORY_BYTE_COUNT, String.valueOf(MAX_DATABASE_MEMORY_BYTE_COUNT_LOWER_BOUND));
            _userInputMap.put(ConfigurationPropertiesExporter.MAX_UTXO_CACHE_BYTE_COUNT, String.valueOf(MAX_UTXO_CACHE_BYTE_COUNT_LOWER_BOUND));
            _userInputMap.put(ConfigurationPropertiesExporter.MIN_PEER_COUNT, String.valueOf(MIN_PEER_COUNT_LOWER_BOUND));
            _userInputMap.put(ConfigurationPropertiesExporter.MAX_PEER_COUNT, String.valueOf(MAX_PEER_COUNT_LOWER_BOUND));

            return;
        }

        final long excessMemory = allocatedMemory - RECOMMENDED_MEMORY;
        final float peerCountMemoryMemoryAllocationPercentage;
        if (excessMemory > (MAX_UTXO_CACHE_BYTE_COUNT_UPPER_BOUND - MAX_UTXO_CACHE_BYTE_COUNT_LOWER_BOUND) / MAX_UTXO_CACHE_BYTE_COUNT_EXCESS_PERCENTAGE) {
            peerCountMemoryMemoryAllocationPercentage = PEER_COUNT_EXCESS_PERCENTAGE / (PEER_COUNT_EXCESS_PERCENTAGE + MAX_DATABASE_MEMORY_BYTE_COUNT_EXCESS_PERCENTAGE);
        }
        else {
            peerCountMemoryMemoryAllocationPercentage = PEER_COUNT_EXCESS_PERCENTAGE;
        }

        final long peerCountMemoryExcessAllocation = (long) (peerCountMemoryMemoryAllocationPercentage * excessMemory);
        final long maxUtxoCacheByteCountExcessAllocation = (long) (excessMemory * MAX_UTXO_CACHE_BYTE_COUNT_EXCESS_PERCENTAGE);
        final long maxDatabaseByteCountExcessAllocation = (long) (excessMemory * MAX_DATABASE_MEMORY_BYTE_COUNT_EXCESS_PERCENTAGE);

        final long maxPeers = Math.min(MAX_PEER_COUNT_LOWER_BOUND + (peerCountMemoryExcessAllocation / BYTES_PER_PEER), MAX_PEER_COUNT_UPPER_BOUND);
        final long minPeers = _clamp(maxPeers / MIN_PEER_COUNT_DIVISOR, MIN_PEER_COUNT_LOWER_BOUND, MIN_PEER_COUNT_UPPER_BOUND);
        final long maxUtxoCacheByteCount = Math.min(MAX_UTXO_CACHE_BYTE_COUNT_LOWER_BOUND + maxUtxoCacheByteCountExcessAllocation, MAX_UTXO_CACHE_BYTE_COUNT_UPPER_BOUND);
        final long maxDatabaseMemoryByteCount = MAX_DATABASE_MEMORY_BYTE_COUNT_LOWER_BOUND + maxDatabaseByteCountExcessAllocation;

        System.out.printf("Min Peers: %d%n", minPeers);
        System.out.printf("Max Peers: %d%n", maxPeers);
        System.out.printf("Max UTXO Cache Bytes: %d%n", maxUtxoCacheByteCount);
        System.out.printf("Max Database Memory Bytes: %d%n", maxDatabaseMemoryByteCount);

        _userInputMap.put("bitcoin." + ConfigurationPropertiesExporter.MAX_MEMORY_BYTE_COUNT, String.valueOf(maxDatabaseMemoryByteCount));
        _userInputMap.put(ConfigurationPropertiesExporter.MAX_UTXO_CACHE_BYTE_COUNT, String.valueOf(maxUtxoCacheByteCount));
        _userInputMap.put(ConfigurationPropertiesExporter.MIN_PEER_COUNT, String.valueOf(minPeers));
        _userInputMap.put(ConfigurationPropertiesExporter.MAX_PEER_COUNT, String.valueOf(maxPeers));
    }

    private long _clamp(final long value, final long min, final long max) {
        return Math.min(Math.max(min, value), max);
    }

    private long _readMemoryValue(final long defaultValue) throws Exception {
        System.out.println("Enter bytes (<Integer Value>[K|M|G]):");
        final String userInput = _bufferedReader.readLine();

        if (userInput == null || userInput.isEmpty()) {
            return defaultValue;
        }

        final String[] splitUserInput = userInput.split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
        if (splitUserInput.length > 2) {
            System.out.println("Unable to read input. Please specify one valid integer value and one (optional) valid memory unit.");
            return _readMemoryValue(defaultValue);
        }

        final long value;
        try {
            value = Long.parseLong(splitUserInput[0]);
        }
        catch (final NumberFormatException exception) {
            System.out.println("Unable to read input. Please specify a valid integer value.");
            return _readMemoryValue(defaultValue);
        }

        if (splitUserInput.length == 1) {
            return value;
        }

        final String unit = splitUserInput[1].trim().toUpperCase();
        switch (unit) {
            case "K":
                return  value * KIB_BYTES;
            case "M":
                return value * MIB_BYTES;
            case "G":
                return value * GIB_BYTES;
            default:
                System.out.println("Unable to read input. Please specify a valid memory unit.");
                return _readMemoryValue(defaultValue);
        }
    }
}
