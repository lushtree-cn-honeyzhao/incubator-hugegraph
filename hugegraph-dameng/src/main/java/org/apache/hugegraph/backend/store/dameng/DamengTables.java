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

import com.google.common.collect.ImmutableMap;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.*;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.backend.store.TableDefine;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DamengTables {

    public static final String BOOLEAN = "TINYINT"; //0 ,1
    public static final String TINYINT = "TINYINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String NUMERIC = "DOUBLE";
    public static final String SMALL_TEXT = "SMALL_TEXT";
    public static final String MID_TEXT = "MID_TEXT";
    public static final String LARGE_TEXT = "LARGE_TEXT";
    // Just used for task input and result
    public static final String HUGE_TEXT = "HUGE_TEXT";

    private static final String DATATYPE_PK = INT;
    private static final String DATATYPE_SL = INT; // VL/EL
    private static final String DATATYPE_IL = INT;

    private static final String SMALL_JSON = MID_TEXT;
    private static final String LARGE_JSON = LARGE_TEXT;

    private static final Map<String, String> TYPES_MAPPING = ImmutableMap.of(
            SMALL_TEXT, "VARCHAR(255)",
            MID_TEXT, "VARCHAR(1024)",
            LARGE_TEXT, "TEXT",
            HUGE_TEXT, "TEXT"
    );

    public static class DamengTableTemplate extends DamengTable {

        protected TableDefine define;

        public DamengTableTemplate(String table) {
            super(table);
        }
        @Override
        public void init(DamengSessions.Session session) {
            super.init(session);
        }

        @Override
        public TableDefine tableDefine() {
            return this.define;
        }
    }

    public static class Meta extends DamengTableTemplate {

        public static final String TABLE = HugeType.META.string();

        public Meta() {
            this(TYPES_MAPPING);
        }

        public Meta(Map<String, String> typesMapping) {
            super(TABLE);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            this.define.column(HugeKeys.VALUE, MID_TEXT);
            this.define.keys(HugeKeys.NAME);
        }

        public void writeVersion(DamengSessions.Session session, String driverVersion) {
            String versionColumn = formatKey(HugeKeys.VERSION);
            String insert = String.format("INSERT INTO %s VALUES " +
                                          "('%s', '%s')", this.table(),
                                          versionColumn, driverVersion);
            try {
                session.execute(insert);
            } catch (SQLException e) {
                throw new BackendException("Failed to insert driver version " +
                                           "with '%s'", e, insert);
            }
        }

        public String readVersion(DamengSessions.Session session) {
            String select = String.format("SELECT %s FROM %s WHERE %s = '%s'",
                                          formatKey(HugeKeys.VALUE),
                                          this.table(), formatKey(HugeKeys.NAME),
                                          formatKey(HugeKeys.VERSION));
            try (ResultSetWrapper results = session.select(select)) {
                ResultSet rs = results.resultSet();
                if (!rs.next()) {
                    return null;
                }
                return rs.getString(formatKey(HugeKeys.VALUE));
            } catch (SQLException e) {
                throw new BackendException(
                          "Failed to get stored version with '%s'", e, select);
            }
        }
    }

    public static class Counters extends DamengTableTemplate {

        public static final String TABLE = HugeType.COUNTER.string();

        public Counters() {
            this(TYPES_MAPPING);
        }

        public Counters(Map<String, String> typesMapping) {
            super(TABLE);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.SCHEMA_TYPE, SMALL_TEXT);
            this.define.column(HugeKeys.ID, INT);
            this.define.keys(HugeKeys.SCHEMA_TYPE);
        }

        public long getCounter(DamengSessions.Session session, HugeType type) {
            String schemaCol = formatKey(HugeKeys.SCHEMA_TYPE);
            String idCol = formatKey(HugeKeys.ID);

            String select = String.format("SELECT ID FROM %s WHERE %s = '%s';",
                                          this.table(), schemaCol, type.name());
            try (ResultSetWrapper results = session.select(select)) {
                ResultSet rs = results.resultSet();
                if (rs.next()) {
                    return rs.getLong(idCol);
                } else {
                    return 0L;
                }
            } catch (SQLException e) {
                throw new BackendException(
                          "Failed to get id from counters with type '%s'",
                          e, type);
            }
        }

        public void increaseCounter(DamengSessions.Session session,
                                    HugeType type, long increment) {
            String update = String.format(
                            "INSERT INTO %s VALUES ('%s', %s) " +
                            "ON DUPLICATE KEY UPDATE ID = ID + %s;",
                            this.table(), type.name(), increment, increment);
            try {
                session.execute(update);
            } catch (SQLException e) {
                throw new BackendException("Failed to update counters " +
                                           "with '%s'", e, update);
            }
        }
    }

    public static class VertexLabel extends DamengTableTemplate {

        public static final String TABLE = HugeType.VERTEX_LABEL.string();

        public VertexLabel() {
            this(TYPES_MAPPING);
        }

        public VertexLabel(Map<String, String> typesMapping) {
            super(TABLE);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            this.define.column(HugeKeys.ID_STRATEGY, TINYINT);
            this.define.column(HugeKeys.PRIMARY_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.column(HugeKeys.TTL, INT);
            this.define.column(HugeKeys.TTL_START_TIME, DATATYPE_PK);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class EdgeLabel extends DamengTableTemplate {

        public static final String TABLE = HugeType.EDGE_LABEL.string();

        public EdgeLabel() {
            this(TYPES_MAPPING);
        }

        public EdgeLabel(Map<String, String> typesMapping) {
            super(TABLE);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            this.define.column(HugeKeys.FREQUENCY, TINYINT);
            this.define.column(HugeKeys.SOURCE_LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.TARGET_LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.column(HugeKeys.TTL, INT);
            this.define.column(HugeKeys.TTL_START_TIME, DATATYPE_PK);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class PropertyKey extends DamengTableTemplate {

        public static final String TABLE = HugeType.PROPERTY_KEY.string();

        public PropertyKey() {
            this(TYPES_MAPPING);
        }

        public PropertyKey(Map<String, String> typesMapping) {
            super(TABLE);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.ID, DATATYPE_PK);
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            this.define.column(HugeKeys.DATA_TYPE, TINYINT);
            this.define.column(HugeKeys.CARDINALITY, TINYINT);
            this.define.column(HugeKeys.AGGREGATE_TYPE, TINYINT);
            this.define.column(HugeKeys.WRITE_TYPE, TINYINT);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class IndexLabel extends DamengTableTemplate {

        public static final String TABLE = HugeType.INDEX_LABEL.string();

        public IndexLabel() {
            this(TYPES_MAPPING);
        }

        public IndexLabel(Map<String, String> typesMapping) {
            super(TABLE);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.ID, DATATYPE_IL);
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            this.define.column(HugeKeys.BASE_TYPE, TINYINT);
            this.define.column(HugeKeys.BASE_VALUE, DATATYPE_SL);
            this.define.column(HugeKeys.INDEX_TYPE, TINYINT);
            this.define.column(HugeKeys.FIELDS, SMALL_JSON);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Vertex extends DamengTableTemplate {

        public static final String TABLE = HugeType.VERTEX.string();

        public Vertex(String store) {
            this(store, TYPES_MAPPING);
        }

        public Vertex(String store, Map<String, String> typesMapping) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.ID, SMALL_TEXT);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.PROPERTIES, HUGE_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Edge extends DamengTableTemplate {

        public static final String TABLE_SUFFIX = HugeType.EDGE.string();

        private final Directions direction;
        private final String delByLabelTemplate;

        public Edge(String store, Directions direction) {
            this(store, direction, TYPES_MAPPING);
        }

        public Edge(String store, Directions direction,
                    Map<String, String> typesMapping) {
            super(joinTableName(store, table(direction)));

            this.direction = direction;
            this.delByLabelTemplate = String.format(
                                      "DELETE FROM %s WHERE %s = ?;",
                                      this.table(), formatKey(HugeKeys.LABEL));

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.OWNER_VERTEX, SMALL_TEXT);
            this.define.column(HugeKeys.DIRECTION, TINYINT);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.OTHER_VERTEX, SMALL_TEXT);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.OWNER_VERTEX, HugeKeys.DIRECTION,
                             HugeKeys.LABEL, HugeKeys.SORT_VALUES,
                             HugeKeys.OTHER_VERTEX);
        }

        @Override
        public List<Object> idColumnValue(Id id) {
            EdgeId edgeId;
            if (id instanceof EdgeId) {
                edgeId = (EdgeId) id;
            } else {
                String[] idParts = EdgeId.split(id);
                if (idParts.length == 1) {
                    // Delete edge by label
                    return Arrays.asList((Object[]) idParts);
                }
                id = IdUtil.readString(id.asString());
                edgeId = EdgeId.parse(id.asString());
            }

            E.checkState(edgeId.direction() == this.direction,
                         "Can't query %s edges from %s edges table",
                         edgeId.direction(), this.direction);

            List<Object> list = new ArrayList<>(5);
            list.add(IdUtil.writeStoredString(edgeId.ownerVertexId()));
            list.add(edgeId.directionCode());
            list.add(edgeId.edgeLabelId().asLong());
            list.add(edgeId.sortValues());
            list.add(IdUtil.writeStoredString(edgeId.otherVertexId()));
            return list;
        }

        @Override
        public void delete(DamengSessions.Session session, DamengBackendEntry.Row entry) {
            // Let super class do delete if not deleting edge by label
            List<Object> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0) {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            this.deleteEdgesByLabel(session, entry.id());
        }

        private void deleteEdgesByLabel(DamengSessions.Session session, Id label) {
            PreparedStatement deleteStmt;
            try {
                // Create or get delete prepare statement
                deleteStmt = session.prepareStatement(this.delByLabelTemplate);
                // Delete edges
                deleteStmt.setObject(1, label.asLong());
            } catch (SQLException e) {
                throw new BackendException("Failed to prepare statement '%s'",
                                           this.delByLabelTemplate);
            }
            session.add(deleteStmt);
        }

        @Override
        public BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex
            // TODO: merge rows before calling row2Entry()

            DamengBackendEntry current = (DamengBackendEntry) e1;
            DamengBackendEntry next = (DamengBackendEntry) e2;

            E.checkState(current == null || current.type().isVertex(),
                         "The current entry must be null or VERTEX");
            E.checkState(next != null && next.type().isEdge(),
                         "The next entry must be EDGE");

            long maxSize = BackendEntryIterator.INLINE_BATCH_SIZE;
            if (current != null && current.subRows().size() < maxSize) {
                Id nextVertexId = IdGenerator.of(
                                  next.<String>column(HugeKeys.OWNER_VERTEX));
                if (current.id().equals(nextVertexId)) {
                    current.subRow(next.row());
                    return current;
                }
            }

            return this.wrapByVertex(next);
        }

        private DamengBackendEntry wrapByVertex(DamengBackendEntry edge) {
            assert edge.type().isEdge();
            String ownerVertex = edge.column(HugeKeys.OWNER_VERTEX);
            E.checkState(ownerVertex != null, "Invalid backend entry");
            Id vertexId = IdGenerator.of(ownerVertex);
            DamengBackendEntry vertex = new DamengBackendEntry(HugeType.VERTEX,
                                                             vertexId);

            vertex.column(HugeKeys.ID, ownerVertex);
            vertex.column(HugeKeys.PROPERTIES, "");

            vertex.subRow(edge.row());
            return vertex;
        }

        public static String table(Directions direction) {
            assert direction == Directions.OUT || direction == Directions.IN;
            return direction.type().string() + TABLE_SUFFIX;
        }

        public static DamengTable out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static DamengTable in(String store) {
            return new Edge(store, Directions.IN);
        }
    }

    public abstract static class Index extends DamengTableTemplate {

        public Index(String table) {
            super(table);
        }

        protected abstract String entryId(DamengBackendEntry entry);
    }

    public static class SecondaryIndex extends Index {

        public static final String TABLE = HugeType.SECONDARY_INDEX.string();

        public SecondaryIndex(String store) {
            this(store, TABLE, TYPES_MAPPING);
        }

        public SecondaryIndex(String store, String table,
                              Map<String, String> typesMapping) {
            super(joinTableName(store, table));

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.FIELD_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.FIELD_VALUES,
                             HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.ELEMENT_IDS);
        }

        @Override
        public final String entryId(DamengBackendEntry entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(fieldValues, labelId.toString());
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public static final String TABLE = HugeType.SEARCH_INDEX.string();

        public SearchIndex(String store) {
            super(store, TABLE, TYPES_MAPPING);
        }
    }

    /**
     * TODO: set field value as key and set element id as value
     */
    public static class UniqueIndex extends SecondaryIndex {

        public static final String TABLE = HugeType.UNIQUE_INDEX.string();

        public UniqueIndex(String store) {
            super(store, TABLE, TYPES_MAPPING);
        }
    }

    public static class RangeIndex extends Index {

        public RangeIndex(String store, String table) {
            this(store, table, TYPES_MAPPING);
        }

        public RangeIndex(String store, String table,
                          Map<String, String> typesMapping) {
            super(joinTableName(store, table));

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, NUMERIC);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.FIELD_VALUES,
                             HugeKeys.ELEMENT_IDS);
        }

        @Override
        public final String entryId(DamengBackendEntry entry) {
            Double fieldValue = entry.<Double>column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(labelId.toString(),
                                              fieldValue.toString());
        }
    }

    public static class RangeIntIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_INT_INDEX.string();

        public RangeIntIndex(String store) {
            this(store, TABLE, TYPES_MAPPING);
        }

        public RangeIntIndex(String store, String table,
                             Map<String, String> typesMapping) {
            super(store, table);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, INT);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.FIELD_VALUES,
                             HugeKeys.ELEMENT_IDS);
        }
    }

    public static class RangeFloatIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_FLOAT_INDEX.string();

        public RangeFloatIndex(String store) {
            this(store, TABLE, TYPES_MAPPING);
        }

        public RangeFloatIndex(String store, String table,
                               Map<String, String> typesMapping) {
            super(store, table);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, NUMERIC);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.FIELD_VALUES,
                             HugeKeys.ELEMENT_IDS);
        }
    }

    public static class RangeLongIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_LONG_INDEX.string();

        public RangeLongIndex(String store) {
            this(store, TABLE, TYPES_MAPPING);
        }

        public RangeLongIndex(String store, String table,
                              Map<String, String> typesMapping) {
            super(store, table);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, BIGINT);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.FIELD_VALUES,
                             HugeKeys.ELEMENT_IDS);
        }
    }

    public static class RangeDoubleIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_DOUBLE_INDEX.string();

        public RangeDoubleIndex(String store) {
            this(store, TABLE, TYPES_MAPPING);
        }

        public RangeDoubleIndex(String store, String table,
                                Map<String, String> typesMapping) {
            super(store, table);

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, NUMERIC);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.FIELD_VALUES,
                             HugeKeys.ELEMENT_IDS);
        }
    }

    public static class ShardIndex extends Index {

        public static final String TABLE = HugeType.SHARD_INDEX.string();

        public ShardIndex(String store) {
            this(store, TYPES_MAPPING);
        }

        public ShardIndex(String store, Map<String, String> typesMapping) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine(typesMapping);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                             HugeKeys.FIELD_VALUES,
                             HugeKeys.ELEMENT_IDS);
        }

        @Override
        public final String entryId(DamengBackendEntry entry) {
            Double fieldValue = entry.<Double>column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(labelId.toString(),
                                              fieldValue.toString());
        }
    }
}
