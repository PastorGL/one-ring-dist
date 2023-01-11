/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.jdbc;

import ash.nazg.data.BinRec;
import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterMeta;
import ash.nazg.metadata.DataHolder;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.InputAdapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

import static ash.nazg.storage.jdbc.JDBCStorage.*;

@SuppressWarnings("unused")
public class JDBCInput extends InputAdapter {
    private JavaSparkContext ctx;
    private int partCount;
    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private String delimiter;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("jdbc", "JDBC adapter for reading data from an SQL SELECT query against" +
                " a configured database. Must use numeric boundaries for each part denoted by two ? placeholders," +
                " from 0 to " + PART_COUNT + ". For example, SELECT *, weeknum - 1 AS part_num FROM table WHERE part_num BETWEEN ? AND ?",

                new DefinitionMetaBuilder()
                        .def(JDBC_DRIVER, "JDBC driver, fully qualified class name")
                        .def(JDBC_URL, "JDBC connection string URL")
                        .def(JDBC_USER, "JDBC connection user", null, "By default, user isn't set")
                        .def(JDBC_PASSWORD, "JDBC connection password", null, "By default, use no password")
                        .def(PART_COUNT, "Desired number of parts",
                                Integer.class, 1, "By default, one part")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        dbDriver = resolver.get(JDBC_DRIVER);
        dbUrl = resolver.get(JDBC_URL);
        dbUser = resolver.get(JDBC_USER);
        dbPassword = resolver.get(JDBC_PASSWORD);

        partCount = resolver.get(PART_COUNT);
    }

    @Override
    public List<DataHolder> load(String query) {
        return Collections.singletonList(new DataHolder(new JdbcRDD<BinRec>(
                        ctx.sc(),
                        new DbConnection(dbDriver, dbUrl, dbUser, dbPassword),
                        query.split(":", 2)[1],
                        0, Math.max(partCount, 0),
                        Math.max(partCount, 1),
                        new BinRecRowMapper(),
                        ClassManifestFactory$.MODULE$.fromClass(BinRec.class)
                ).toJavaRDD(), null)
        );
    }

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {
        final String _dbDriver;
        final String _dbUrl;
        final String _dbUser;
        final String _dbPassword;

        DbConnection(String _dbDriver, String _dbUrl, String _dbUser, String _dbPassword) {
            this._dbDriver = _dbDriver;
            this._dbUrl = _dbUrl;
            this._dbUser = _dbUser;
            this._dbPassword = _dbPassword;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(_dbDriver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            Properties properties = new Properties();
            if (_dbUser != null) {
                properties.setProperty("user", _dbUser);
            }
            if (_dbPassword != null) {
                properties.setProperty("password", _dbPassword);
            }

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(_dbUrl, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return connection;
        }
    }

    static class BinRecRowMapper extends AbstractFunction1<ResultSet, BinRec> implements Serializable {
        @Override
        public BinRec apply(ResultSet row) {
            try {
                ResultSetMetaData metaData = row.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < columnCount; i++) {
                    map.put(metaData.getColumnName(i), row.getObject(i));
                }
                return new BinRec(map);
            } catch (SQLException ignore) {
                return null;
            }
        }
    }
}
