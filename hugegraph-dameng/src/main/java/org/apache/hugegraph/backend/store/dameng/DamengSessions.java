/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.dameng;

import org.apache.http.client.utils.URIBuilder;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class DamengSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(DamengSessions.class);

    private static final String JDBC_PREFIX = "jdbc:";

    private static final int DROP_DB_TIMEOUT = 10000;

    private HugeConfig config;
    private String database;
    private volatile boolean opened;

    public DamengSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
        this.config = config;
        this.database = database;
        this.opened = false;
    }

    @Override
    public HugeConfig config() {
        return this.config;
    }

    public String database() {
        return this.database;
    }

    public String escapedDatabase() {
        return DamengUtil.escapeString(this.database());
    }

    /**
     * Try connect with specified database, will not reconnect if failed
     * @throws SQLException if a database access error occurs
     */
    @Override
    public synchronized void open() throws Exception {
        try (Connection conn = this.open(false)) {
            this.opened = true;
        }
    }

    @Override
    protected boolean opened() {
        return this.opened;
    }

    @Override
    protected void doClose() {
        // pass
    }

    @Override
    public Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    public void createDatabase() {
        // Create database with non-database-session
        LOG.info("Create database: {}", this.database());
        String sql = this.buildCreateDatabase(this.database());
        try {
            Connection conn = this.openWithoutDB(DROP_DB_TIMEOUT);
            conn.createStatement().execute(sql);
            conn.close();
        } catch (SQLException e) {
            if (!e.getMessage().endsWith("already exists")) {
                throw new BackendException("Failed to create database '%s'", e,
                                           this.database());
            }
            // Ignore exception if database already exists
        }
    }

    public void dropDatabase() {
        LOG.info("Drop database: {}", this.database());

        String sql = this.buildDropDatabase(this.database());
        try {
            Connection conn = this.openWithoutDB(DROP_DB_TIMEOUT);
            conn.createStatement().execute(sql);
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            if (e.getCause() instanceof SocketTimeoutException) {
                LOG.warn("Drop database '{}' timeout", this.database());
            } else {
                throw new BackendException("Failed to drop database '%s'", e,
                                           this.database());
            }
        }
    }

    public boolean existsDatabase() {
        try {
            Connection conn = this.openWithoutDB(300);
            ResultSet result = conn.getMetaData().getCatalogs();
            while (result.next()) {
                String dbName = result.getString(1);
                LOG.info(String.format("DamengSessions->existsDatabase->dbName::%s",dbName));
                LOG.info(String.format("DamengSessions->existsDatabase->this.database()::%s",this.database()));
                if (dbName.equals(this.database())) {
                    return true;
                }
            }
            conn.close();
        } catch (Exception e) {
            throw new BackendException("Failed to obtain database info", e);
        }
        return false;
    }

    public boolean existsTable(String table) {
        String sql = this.buildExistsTable(table);
        try (Connection conn = this.openWithDB(30);
             ResultSet result = conn.createStatement().executeQuery(sql)) {
            return result.next();
        } catch (Exception e) {
            throw new BackendException("Failed to obtain table info", e);
        }
    }

    public void resetConnections() {
        // Close the under layer connections owned by each thread
        this.forceResetSessions();
    }

    //CREATE SCHEMA "HUGEGRAPH" AUTHORIZATION "HUGEGRAPH";
    protected String buildCreateDatabase(String database) {
        return String.format("CREATE SCHEMA %s AUTHORIZATION %s;",
                             database,database);
    }

    //DROP SCHEMA "HUGEGRAPH" AUTHORIZATION "HUGEGRAPH";
    protected String buildDropDatabase(String database) {
        return String.format("DROP SCHEMA \"%s\" AUTHORIZATION \"%s\";", database,database);
    }

    protected String buildExistsTable(String table) {
        return String.format("SELECT * FROM SYS.dba_tables WHERE OWNER = '%s' AND TABLE_NAME = '%s';",
                             this.escapedDatabase(),
                             DamengUtil.escapeString(table));
    }

    /**
     * Connect DB without specified database
     */
    protected Connection openWithoutDB(int timeout) {
        String url = this.buildUri(false, false, false, timeout);
        return this.connect(url);
    }

    /**
     * Connect DB with specified database, but won't auto reconnect
     */
    protected Connection openWithDB(int timeout) {
        String url = this.buildUri(false, true, false, timeout);
        return this.connect(url);
    }

    /**
     * Connect DB with specified database
     */
    private Connection open(boolean autoReconnect) throws SQLException {
        String url = this.buildUri(true, true, autoReconnect, null);
        return this.connect(url);
    }

    protected String buildUri(boolean withConnParams, boolean withDB,
                              boolean autoReconnect, Integer timeout) {
        String url = this.buildUrlPrefix(withDB);

        E.checkArgument(url.startsWith(JDBC_PREFIX),
                        "The url must start with '%s': '%s'",
                        JDBC_PREFIX, url);
        String urlWithoutJdbc = url.substring(JDBC_PREFIX.length());
        URIBuilder builder;
        try {
            builder = this.newConnectionURIBuilder(urlWithoutJdbc);
        } catch (URISyntaxException e) {
            throw new BackendException("Invalid url '%s'", e, url);
        }


        if (withConnParams) {
            builder.setParameter("characterEncoding", "utf-8")
                   .setParameter("rewriteBatchedStatements", "true")
                   .setParameter("useServerPrepStmts", "false");
        }
        if (timeout != null) {
            builder.setParameter("socketTimeout", String.valueOf(timeout));
        }


        return JDBC_PREFIX + builder.toString();
    }

    protected String buildUrlPrefix(boolean withDB) {
        String url = this.config.get(DamengOptions.JDBC_URL);
        if (!url.endsWith("/")) {
            url = String.format("%s/", url);
        }
        String database = withDB ? this.database() : this.connectDatabase();
        return String.format("%s%s", url, database);
    }

    protected String connectDatabase() {
        return Strings.EMPTY;
    }

    protected URIBuilder newConnectionURIBuilder(String url)
                                                 throws URISyntaxException {
        return new URIBuilder(url);
    }

    private Connection connect(String url){
        LOG.info("Connect to the jdbc url: '{}'", url);
        String driverName = this.config.get(DamengOptions.JDBC_DRIVER);
        String username = this.config.get(DamengOptions.JDBC_USERNAME);
        String password = this.config.get(DamengOptions.JDBC_PASSWORD);
        try {
            // Register JDBC driver
            Class.forName(driverName);

            return DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        LOG.info("===============connect null !!!!");
        return null;
    }

    public class Session extends AbstractBackendSession {

        private Connection conn;
        private Map<String, PreparedStatement> statements;
        private int count;

        public Session() {
            this.conn = null;
            this.statements = new HashMap<>();
            this.count = 0;
        }

        public HugeConfig config() {
            return DamengSessions.this.config();
        }

        @Override
        public void open() {
            try {
                this.doOpen();
            } catch (SQLException e) {
                throw new BackendException("Failed to open connection", e);
            }
        }

        private void tryOpen() {
            try {
                this.doOpen();
            } catch (SQLException ignored) {
                // Ignore
            }
        }

        private void doOpen() throws SQLException {
            this.opened = true;
            if (this.conn != null && !this.conn.isClosed()) {
                return;
            }
            this.conn = DamengSessions.this.open(true);
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.conn == null) {
                return;
            }

            this.opened = false;
            this.doClose();
        }

        private void doClose() {
            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    exception = e;
                }
            }
            this.statements.clear();

            try {
                this.conn.close();
            } catch (SQLException e) {
                exception = e;
            } finally {
                this.conn = null;
            }

            if (exception != null) {
                throw new BackendException("Failed to close connection",
                                           exception);
            }
        }

        @Override
        public boolean opened() {
            if (this.opened && this.conn == null) {
                // Reconnect if the connection is reset
                tryOpen();
            }
            return this.opened && this.conn != null;
        }

        @Override
        public boolean closed() {
            if (!this.opened || this.conn == null) {
                return true;
            }
            try {
                return this.conn.isClosed();
            } catch (SQLException ignored) {
                // Assume closed here
                return true;
            }
        }

        public void clear() {
            this.count = 0;
            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.clearBatch();
                } catch (SQLException e) {
                    exception = e;
                }
            }
            if (exception != null) {
                /*
                 * Will throw exception when the database connection error,
                 * we clear statements because clearBatch() failed
                 */
                this.statements = new HashMap<>();
            }
        }

        public void begin() throws SQLException {
            this.conn.setAutoCommit(false);
        }

        public void end() throws SQLException {
            this.conn.setAutoCommit(true);
        }

        public void endAndLog() {
            try {
                this.conn.setAutoCommit(true);
            } catch (SQLException e) {
                LOG.warn("Failed to set connection to auto-commit status", e);
            }
        }

        @Override
        public Integer commit() {
            int updated = 0;
            try {
                for (PreparedStatement statement : this.statements.values()) {
                    updated += IntStream.of(statement.executeBatch()).sum();
                }
                this.conn.commit();
                this.clear();
            } catch (SQLException e) {
                throw new BackendException("Failed to commit", e);
            }
            /*
             * Can't call endAndLog() in `finally` block here.
             * Because If commit already failed with an exception,
             * then rollback() should be called. Besides rollback() can only
             * be called when autocommit=false and rollback() will always set
             * autocommit=true. Therefore only commit successfully should set
             * autocommit=true here
             */
            this.endAndLog();
            return updated;
        }

        @Override
        public void rollback() {
            this.clear();
            try {
                this.conn.rollback();
            } catch (SQLException e) {
                throw new BackendException("Failed to rollback", e);
            } finally {
                this.endAndLog();
            }
        }

        @Override
        public boolean hasChanges() {
            return this.count > 0;
        }

        @Override
        public void reconnectIfNeeded() {
            if (!this.opened) {
                return;
            }

            if (this.conn == null) {
                tryOpen();
            }

            try {
                this.execute("SELECT 1;");
            } catch (SQLException ignored) {
                // pass
            }
        }

        @Override
        public void reset() {
            // NOTE: this method may be called by other threads
            if (this.conn == null) {
                return;
            }
            try {
                this.doClose();
            } catch (Throwable e) {
                LOG.warn("Failed to reset connection", e);
            }
        }

        public ResultSetWrapper select(String sql) throws SQLException {
            assert this.conn.getAutoCommit();
            Statement statement = this.conn.createStatement();
            try {
                ResultSet rs = statement.executeQuery(sql);
                return new ResultSetWrapper(rs, statement);
            } catch (SQLException e) {
                statement.close();
                throw e;
            }
        }

        public boolean execute(String sql) throws SQLException {
            /*
             * commit() or rollback() failed to set connection to auto-commit
             * status in prior transaction. Manually set to auto-commit here.
             */
            if (!this.conn.getAutoCommit()) {
                this.end();
            }

            try (Statement statement = this.conn.createStatement()) {
                return statement.execute(sql);
            }
        }

        public void add(PreparedStatement statement) {
            try {
                // Add a row to statement
                statement.addBatch();
                this.count++;
            } catch (SQLException e) {
                throw new BackendException("Failed to add statement '%s' " +
                                           "to batch", e, statement);
            }
        }

        public PreparedStatement prepareStatement(String sqlTemplate)
                                                  throws SQLException {
            PreparedStatement statement = this.statements.get(sqlTemplate);
            if (statement == null) {
                statement = this.conn.prepareStatement(sqlTemplate);
                this.statements.putIfAbsent(sqlTemplate, statement);
            }
            return statement;
        }
    }
}
