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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.AbstractBackendStore;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.dameng.DamengSessions.Session;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;

public abstract class DamengStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(DamengStore.class);

    private static final BackendFeatures FEATURES = new DamengFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;

    private final Map<HugeType, DamengTable> tables;

    private DamengSessions sessions;

    public DamengStore(final BackendStoreProvider provider,
                      final String database, final String store) {
        E.checkNotNull(database, "database");
        E.checkNotNull(store, "store");
        this.provider = provider;
        this.database = database;
        this.store = store;

        this.sessions = null;
        this.tables = new ConcurrentHashMap<>();

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            DamengMetrics metrics = new DamengMetrics();
            return metrics.metrics();
        });
    }

    protected void registerTableManager(HugeType type, DamengTable table) {
        this.tables.put(type, table);
    }

    protected DamengSessions openSessionPool(HugeConfig config) {
        return new DamengSessions(config, this.database, this.store);
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        this.sessions = this.openSessionPool(config);

        if (this.sessions.existsDatabase()) {
            LOG.debug("Store connect with database: {}", this.database);
            try {
                this.sessions.open();
            } catch (Throwable e) {
                throw new ConnectionException("Failed to connect to Dameng", e);
            }

            try {
                this.sessions.session().open();
            } catch (Throwable e) {
                try {
                    this.sessions.close();
                } catch (Throwable e2) {
                    LOG.warn("Failed to close connection after an error", e2);
                }
                throw new BackendException("Failed to open database", e);
            }
        } else {
            if (this.isSchemaStore()) {
                LOG.info("Failed to open database '{}', " +
                         "try to init database later", this.database);
            }
            this.sessions.session();
        }

        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.checkClusterConnected();
        this.sessions.close();
    }

    @Override
    public boolean opened() {
        this.checkClusterConnected();
        return this.sessions.session().opened();
    }

    @Override
    public void init() {
        this.checkClusterConnected();
        this.sessions.createDatabase();
        try {
            // Open a new session connected with specified database
            this.sessions.session().open();
        } catch (Exception e) {
            throw new BackendException("Failed to connect database '%s'",
                                       this.database);
        }
        this.checkOpened();
        this.initTables();

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        // Check connected
        this.checkClusterConnected();

        if (this.sessions.existsDatabase()) {
            if (!clearSpace) {
                this.checkOpened();
                this.clearTables();
                /*
                 * Disconnect connections for following database drop.
                 * Connections will be auto reconnected if not drop database
                 * in next step, but never do this operation because database
                 * might be blocked in Dameng or throw 'terminating' exception.
                 * we can't resetConnections() when dropDatabase(), because
                 * there are 3 stores(schema,system,graph), which are shared
                 * one database, other stores may keep connected with the
                 * database when one store doing clear(clearSpace=false).
                 */
                this.sessions.resetConnections();
            } else {
                this.sessions.dropDatabase();
            }
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        this.checkClusterConnected();

        if (!this.sessions.existsDatabase()) {
            return false;
        }
        for (DamengTable table : this.tables()) {
            if (!this.sessions.existsTable(table.table())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void truncate() {
        this.checkOpened();

        this.truncateTables();
        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(Session session, BackendAction item) {
        DamengBackendEntry entry = castBackendEntry(item.entry());
        DamengTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry.row());
                break;
            case DELETE:
                table.delete(session, entry.row());
                break;
            case APPEND:
                table.append(session, entry.row());
                break;
            case ELIMINATE:
                table.eliminate(session, entry.row());
                break;
            case UPDATE_IF_PRESENT:
                table.updateIfPresent(session, entry.row());
                break;
            case UPDATE_IF_ABSENT:
                table.updateIfAbsent(session, entry.row());
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();

        DamengTable table = this.table(DamengTable.tableType(query));
        return table.query(this.sessions.session(), query);
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        DamengTable table = this.table(DamengTable.tableType(query));
        return table.queryNumber(this.sessions.session(), query);
    }

    @Override
    public void beginTx() {
        this.checkOpened();

        Session session = this.sessions.session();
        try {
            session.begin();
        } catch (SQLException e) {
            throw new BackendException("Failed to open transaction", e);
        }
    }

    @Override
    public void commitTx() {
        this.checkOpened();

        Session session = this.sessions.session();
        int count = session.commit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} committed {} items", this.store, count);
        }
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        Session session = this.sessions.session();
        session.rollback();
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    protected void initTables() {
        Session session = this.sessions.session();
        for (DamengTable table : this.tables()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        Session session = this.sessions.session();
        for (DamengTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        Session session = this.sessions.session();
        for (DamengTable table : this.tables()) {
            table.truncate(session);
        }
    }

    protected Collection<DamengTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final DamengTable table(HugeType type) {
        assert type != null;
        DamengTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    protected final void checkClusterConnected() {
        E.checkState(this.sessions != null,
                     "Dameng store has not been initialized");
    }

    protected static DamengBackendEntry castBackendEntry(BackendEntry entry) {
        if (!(entry instanceof DamengBackendEntry)) {
            throw new BackendException(
                      "Dameng store only supports DamengBackendEntry");
        }
        return (DamengBackendEntry) entry;
    }

    public static class DamengSchemaStore extends DamengStore {

        private final DamengTables.Counters counters;

        public DamengSchemaStore(BackendStoreProvider provider,
                                String database, String store) {
            super(provider, database, store);

            this.counters = new DamengTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new DamengTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new DamengTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new DamengTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new DamengTables.IndexLabel());
        }

        @Override
        protected Collection<DamengTable> tables() {
            List<DamengTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.checkOpened();
            Session session = super.sessions.session();
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkOpened();
            Session session = super.sessions.session();
            return this.counters.getCounter(session, type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class DamengGraphStore extends DamengStore {

        public DamengGraphStore(BackendStoreProvider provider,
                               String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                                 new DamengTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                                 DamengTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                                 DamengTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new DamengTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new DamengTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new DamengTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new DamengTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new DamengTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new DamengTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new DamengTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new DamengTables.UniqueIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("DamengGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "DamengGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "DamengGraphStore.getCounter()");
        }
    }

    public static class DamengSystemStore extends DamengGraphStore {

        private final DamengTables.Meta meta;

        public DamengSystemStore(BackendStoreProvider provider,
                                String database, String store) {
            super(provider, database, store);

            this.meta = new DamengTables.Meta();
        }

        @Override
        public void init() {
            super.init();
            Session session = super.session(null);
            String driverVersion = this.provider().driverVersion();
            this.meta.writeVersion(session, driverVersion);
            LOG.info("Write down the backend version: {}", driverVersion);
        }

        @Override
        public String storedVersion() {
            super.init();
            Session session = super.session(null);
            return this.meta.readVersion(session);
        }

        @Override
        protected Collection<DamengTable> tables() {
            List<DamengTable> tables = new ArrayList<>(super.tables());
            tables.add(this.meta);
            return tables;
        }
    }
}
