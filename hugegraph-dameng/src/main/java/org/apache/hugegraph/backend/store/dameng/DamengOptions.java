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

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

public class DamengOptions extends OptionHolder {

    protected DamengOptions() {
        super();
    }

    private static volatile DamengOptions instance;

    public static synchronized DamengOptions instance() {
        if (instance == null) {
            instance = new DamengOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> JDBC_DRIVER =
            new ConfigOption<>(
                    "jdbc.driver",
                    "The JDBC driver class to connect database.",
                    disallowEmpty(),
                    "dm.jdbc.driver.DmDriver"
            );

    public static final ConfigOption<String> JDBC_URL =
            new ConfigOption<>(
                    "jdbc.url",
                    "The url of database in JDBC format.",
                    disallowEmpty(),
                    "jdbc:dm://127.0.0.1:5236/HUGEGRAPH?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useUnicode=true&useSSL=false&serverTimezone=UTC"
            );

    public static final ConfigOption<String> JDBC_USERNAME =
            new ConfigOption<>(
                    "jdbc.username",
                    "The username to login database.",
                    disallowEmpty(),
                    "HUGEGRAPH"
            );

    public static final ConfigOption<String> JDBC_PASSWORD =
            new ConfigOption<>(
                    "jdbc.password",
                    "The password corresponding to jdbc.username.",
                    null,
                    "HUGEGRAPH"
            );

    public static final ConfigOption<Boolean> JDBC_FORCED_AUTO_RECONNECT =
            new ConfigOption<>(
                    "jdbc.forced_auto_reconnect",
                    "Whether to forced auto reconnect to the database even " +
                    "if the connection fails at the first time. Note that " +
                    "forced_auto_reconnect=true will disable fail-fast.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> JDBC_RECONNECT_MAX_TIMES =
            new ConfigOption<>(
                    "jdbc.reconnect_max_times",
                    "The reconnect times when the database connection fails.",
                    rangeInt(1, 10),
                    3
            );

    public static final ConfigOption<Integer> JDBC_RECONNECT_INTERVAL =
            new ConfigOption<>(
                    "jdbc.reconnect_interval",
                    "The interval(seconds) between reconnections when the " +
                    "database connection fails.",
                    rangeInt(1, 10),
                    3
            );

    public static final ConfigOption<String> JDBC_SSL_MODE =
            new ConfigOption<>(
                    "jdbc.ssl_mode",
                    "The SSL mode of connections with database.",
                    disallowEmpty(),
                    "false"
            );

    public static final ConfigOption<String> JDBC_STORAGE_ENGINE =
            new ConfigOption<>(
                   "jdbc.storage_engine",
                   "The storage engine of backend store database, " +
                   "like InnoDB/MyISAM/RocksDB for Dameng.",
                    disallowEmpty(),
                    "InnoDB"
            );
}
