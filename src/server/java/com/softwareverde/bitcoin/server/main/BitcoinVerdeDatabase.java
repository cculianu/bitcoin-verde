package com.softwareverde.bitcoin.server.main;

import com.softwareverde.bitcoin.server.configuration.BitcoinVerdeDatabaseProperties;
import com.softwareverde.bitcoin.server.database.Database;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.DatabaseConnectionFactory;
import com.softwareverde.bitcoin.server.database.wrapper.MysqlDatabaseConnectionFactoryWrapper;
import com.softwareverde.bitcoin.server.database.wrapper.MysqlDatabaseConnectionWrapper;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.DatabaseInitializer;
import com.softwareverde.database.mysql.MysqlDatabase;
import com.softwareverde.database.mysql.MysqlDatabaseConnection;
import com.softwareverde.database.mysql.MysqlDatabaseConnectionFactory;
import com.softwareverde.database.mysql.MysqlDatabaseInitializer;
import com.softwareverde.database.mysql.SqlScriptRunner;
import com.softwareverde.database.mysql.embedded.EmbeddedMysqlDatabase;
import com.softwareverde.database.mysql.embedded.properties.EmbeddedDatabaseProperties;
import com.softwareverde.database.properties.DatabaseCredentials;
import com.softwareverde.database.util.TransactionUtil;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.Version;

import java.io.StringReader;
import java.sql.Connection;

public class BitcoinVerdeDatabase implements Database {
    public static class InitFile {
        public final String sqlInitFile;
        public final Integer databaseVersion;

        public InitFile(final String sqlInitFile, final Integer databaseVersion) {
            this.sqlInitFile = sqlInitFile;
            this.databaseVersion = databaseVersion;
        }
    }

    public static final InitFile SPV = new InitFile("/sql/spv/mysql/init.sql", BitcoinConstants.DATABASE_VERSION);
    public static final InitFile BITCOIN = new InitFile("/sql/node/mysql/init.sql", BitcoinConstants.DATABASE_VERSION);
    public static final InitFile STRATUM = new InitFile("/sql/stratum/mysql/init.sql", BitcoinConstants.DATABASE_VERSION);

    public static final Integer MAX_DATABASE_CONNECTION_COUNT = 151; // 64 // Increasing too much may cause MySQL to use excessive memory...

    public static final DatabaseInitializer.DatabaseUpgradeHandler<Connection> DATABASE_UPGRADE_HANDLER = new DatabaseInitializer.DatabaseUpgradeHandler<Connection>() {
        @Override
        public Boolean onUpgrade(final com.softwareverde.database.DatabaseConnection<Connection> maintenanceDatabaseConnection, final Integer currentVersion, final Integer requiredVersion) {
            if ( (currentVersion == 1) || (currentVersion == 2) ) {
                return false; // Upgrading from Verde v1 (DB v1-v2) is not supported.
            }

            int upgradedVersion = currentVersion;

            // v3 -> v4 (Memo Support)
            if ( (currentVersion == 3) && (requiredVersion >= 4) ) {
                Logger.info("[Upgrading DB to v4]");
                final Boolean wasSuccessful = _upgradeDatabaseMemoSupport(maintenanceDatabaseConnection);
                if (! wasSuccessful) { return false; }

                upgradedVersion = 4;
            }

            // v4 -> v5 (Double Spend Proofs Support)
            if ( (currentVersion == 4) && (requiredVersion >= 5) ) {
                Logger.info("[Upgrading DB to v5]");
                final Boolean wasSuccessful = _upgradeDoubleSpendProofsSupport(maintenanceDatabaseConnection);
                if (! wasSuccessful) { return false; }

                upgradedVersion = 5;
            }

            return (upgradedVersion >= requiredVersion);
        }
    };

    public static Database newInstance(final InitFile sqlInitFile, final BitcoinVerdeDatabaseProperties bitcoinVerdeDatabaseProperties) {
        final DatabaseInitializer<Connection> databaseInitializer = new MysqlDatabaseInitializer(sqlInitFile.sqlInitFile, sqlInitFile.databaseVersion, BitcoinVerdeDatabase.DATABASE_UPGRADE_HANDLER);

        try {
            if (bitcoinVerdeDatabaseProperties.shouldUseEmbeddedDatabase()) {
                // Initialize the embedded database...
                final EmbeddedDatabaseProperties embeddedDatabaseProperties = DatabaseConfigurer.configureDatabase(MAX_DATABASE_CONNECTION_COUNT, bitcoinVerdeDatabaseProperties);

                Logger.info("[Initializing Database]");
                final EmbeddedMysqlDatabase embeddedMysqlDatabase = new EmbeddedMysqlDatabase(embeddedDatabaseProperties, databaseInitializer);
                if (Logger.isDebugEnabled()) {
                    final Version installedVersion = embeddedMysqlDatabase.getInstallationDirectoryVersion();
                    Logger.debug("MariaDb Version: " + installedVersion);
                }
                embeddedMysqlDatabase.start();

                final DatabaseCredentials maintenanceCredentials = databaseInitializer.getMaintenanceCredentials(embeddedDatabaseProperties);
                final MysqlDatabaseConnectionFactory maintenanceDatabaseConnectionFactory = new MysqlDatabaseConnectionFactory(embeddedDatabaseProperties, maintenanceCredentials);
                return new BitcoinVerdeDatabase(embeddedMysqlDatabase, maintenanceDatabaseConnectionFactory);
            }
            else {
                // Connect to the remote database...
                final DatabaseCredentials credentials = bitcoinVerdeDatabaseProperties.getCredentials();
                final DatabaseCredentials rootCredentials = bitcoinVerdeDatabaseProperties.getRootCredentials();
                final DatabaseCredentials maintenanceCredentials = databaseInitializer.getMaintenanceCredentials(bitcoinVerdeDatabaseProperties);

                final MysqlDatabaseConnectionFactory rootDatabaseConnectionFactory = new MysqlDatabaseConnectionFactory(bitcoinVerdeDatabaseProperties.getHostname(), bitcoinVerdeDatabaseProperties.getPort(), "", rootCredentials.username, rootCredentials.password);
                final MysqlDatabaseConnectionFactory maintenanceDatabaseConnectionFactory = new MysqlDatabaseConnectionFactory(bitcoinVerdeDatabaseProperties, maintenanceCredentials);
                // final MysqlDatabaseConnectionFactory databaseConnectionFactory = new MysqlDatabaseConnectionFactory(connectionUrl, credentials.username, credentials.password);

                try (final MysqlDatabaseConnection maintenanceDatabaseConnection = maintenanceDatabaseConnectionFactory.newConnection()) {
                    final Integer databaseVersion = databaseInitializer.getDatabaseVersionNumber(maintenanceDatabaseConnection);
                    if (databaseVersion < 0) {
                        try (final MysqlDatabaseConnection rootDatabaseConnection = rootDatabaseConnectionFactory.newConnection()) {
                            databaseInitializer.initializeSchema(rootDatabaseConnection, bitcoinVerdeDatabaseProperties);
                        }
                    }
                }
                catch (final DatabaseException exception) {
                    try (final MysqlDatabaseConnection rootDatabaseConnection = rootDatabaseConnectionFactory.newConnection()) {
                        databaseInitializer.initializeSchema(rootDatabaseConnection, bitcoinVerdeDatabaseProperties);
                    }
                }

                try (final MysqlDatabaseConnection maintenanceDatabaseConnection = maintenanceDatabaseConnectionFactory.newConnection()) {
                    databaseInitializer.initializeDatabase(maintenanceDatabaseConnection);
                }

                final MysqlDatabase mysqlDatabase = new MysqlDatabase(bitcoinVerdeDatabaseProperties, credentials);
                return new BitcoinVerdeDatabase(mysqlDatabase, maintenanceDatabaseConnectionFactory);
            }
        }
        catch (final Exception exception) {
            Logger.error(exception);
        }

        return null;
    }

    public static DatabaseConnectionFactory getMaintenanceDatabaseConnectionFactory(final BitcoinVerdeDatabaseProperties databaseProperties) {
        final DatabaseInitializer<Connection> databaseInitializer = new MysqlDatabaseInitializer();
        final DatabaseCredentials maintenanceCredentials = databaseInitializer.getMaintenanceCredentials(databaseProperties);
        final MysqlDatabaseConnectionFactory databaseConnectionFactory = new MysqlDatabaseConnectionFactory(databaseProperties, maintenanceCredentials);
        return new MysqlDatabaseConnectionFactoryWrapper(databaseConnectionFactory);
    }

    protected static Boolean _upgradeDatabaseMemoSupport(final com.softwareverde.database.DatabaseConnection<Connection> databaseConnection) {
        try {
            final String upgradeScript = IoUtil.getResource("/sql/node/mysql/upgrade/memo_v1.sql"); // TODO: Use mysql/sqlite when appropriate...
            if (Util.isBlank(upgradeScript)) { return false; }

            TransactionUtil.startTransaction(databaseConnection);
            final SqlScriptRunner scriptRunner = new SqlScriptRunner(databaseConnection.getRawConnection(), false, true);
            scriptRunner.runScript(new StringReader(upgradeScript));
            TransactionUtil.commitTransaction(databaseConnection);

            return true;
        }
        catch (final Exception exception) {
            Logger.debug(exception);
            return false;
        }
    }

    protected static Boolean _upgradeDoubleSpendProofsSupport(final com.softwareverde.database.DatabaseConnection<Connection> databaseConnection) {
        try {
            final String upgradeScript = IoUtil.getResource("/sql/node/mysql/upgrade/double_spend_proofs_v1.sql"); // TODO: Use mysql/sqlite when appropriate...
            if (Util.isBlank(upgradeScript)) { return false; }

            TransactionUtil.startTransaction(databaseConnection);
            final SqlScriptRunner scriptRunner = new SqlScriptRunner(databaseConnection.getRawConnection(), false, true);
            scriptRunner.runScript(new StringReader(upgradeScript));
            TransactionUtil.commitTransaction(databaseConnection);

            return true;
        }
        catch (final Exception exception) {
            Logger.debug(exception);
            return false;
        }
    }

    protected final MysqlDatabase _core;
    protected final MysqlDatabaseConnectionFactory _maintenanceDatabaseConnectionFactory;

    protected BitcoinVerdeDatabase(final MysqlDatabase core, final MysqlDatabaseConnectionFactory maintenanceDatabaseConnectionFactory) {
        _core = core;
        _maintenanceDatabaseConnectionFactory = maintenanceDatabaseConnectionFactory;
    }

    @Override
    public DatabaseConnection newConnection() throws DatabaseException {
        return new MysqlDatabaseConnectionWrapper(_core.newConnection());
    }

    @Override
    public DatabaseConnection getMaintenanceConnection() throws DatabaseException {
        if (_maintenanceDatabaseConnectionFactory == null) { return null; }

        final MysqlDatabaseConnection databaseConnection = _maintenanceDatabaseConnectionFactory.newConnection();
        return new MysqlDatabaseConnectionWrapper(databaseConnection);
    }

    @Override
    public void close() {
        if (_core instanceof EmbeddedMysqlDatabase) {
            try {
                ((EmbeddedMysqlDatabase) _core).stop();
            }
            catch (final Exception exception) { }
        }
    }

    @Override
    public DatabaseConnectionFactory newConnectionFactory() {
        return new MysqlDatabaseConnectionFactoryWrapper(_core.newConnectionFactory());
    }

    @Override
    public Integer getMaxQueryBatchSize() {
        return 1024;
    }
}
