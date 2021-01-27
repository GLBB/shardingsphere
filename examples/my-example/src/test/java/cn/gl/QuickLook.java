package cn.gl;

import com.zaxxer.hikari.HikariDataSource;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.sql.parser.api.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.api.SQLVisitorEngine;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class QuickLook {

    DataSource dataSource;



    @Before
    public void init() throws SQLException {
        // Configure actual data sources
        Map<String, DataSource> dataSourceMap = new HashMap<>();

        // Configure the first data source
        HikariDataSource dataSource1 = new HikariDataSource();
        dataSource1.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource1.setJdbcUrl("jdbc:mysql://localhost:3306/ds0");
        dataSource1.setUsername("gl");
        dataSource1.setPassword("123");
        dataSourceMap.put("ds0", dataSource1);

        // Configure the second data source
        HikariDataSource dataSource2 = new HikariDataSource();
        dataSource2.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource2.setJdbcUrl("jdbc:mysql://localhost:3306/ds1");
        dataSource2.setUsername("gl");
        dataSource2.setPassword("123");
        dataSourceMap.put("ds1", dataSource2);

        // Configure order table rule
        ShardingTableRuleConfiguration orderTableRuleConfig = new ShardingTableRuleConfiguration("t_order", "ds${0..1}.t_order${0..1}");
        // Configure database sharding strategy
        orderTableRuleConfig.setDatabaseShardingStrategy(new StandardShardingStrategyConfiguration("user_id", "dbShardingAlgorithm"));
        // Configure table sharding strategy
        orderTableRuleConfig.setTableShardingStrategy(new StandardShardingStrategyConfiguration("order_id", "tableShardingAlgorithm"));

        // Configure sharding rule
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTables().add(orderTableRuleConfig);

        // Configure database sharding algorithm
        Properties dbShardingAlgorithmrProps = new Properties();
        dbShardingAlgorithmrProps.setProperty("algorithm-expression", "ds${user_id % 2}");
        shardingRuleConfig.getShardingAlgorithms().put("dbShardingAlgorithm", new ShardingSphereAlgorithmConfiguration("INLINE", dbShardingAlgorithmrProps));

        // Configure table sharding algorithm
        Properties tableShardingAlgorithmrProps = new Properties();
        tableShardingAlgorithmrProps.setProperty("algorithm-expression", "t_order${order_id % 2}");
        shardingRuleConfig.getShardingAlgorithms().put("tableShardingAlgorithm", new ShardingSphereAlgorithmConfiguration("INLINE", tableShardingAlgorithmrProps));

        // Create ShardingSphereDataSource
        dataSource = ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, Collections.singleton(shardingRuleConfig), new Properties());
    }

    @Test
    public void hello() throws SQLException {
        String sql = "insert into t_order(order_id, user_id, status) \n" +
            "values \n" +
            "(1, 2, \"q\")";

        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        boolean isResultSet = statement.execute(sql);
        System.out.println(isResultSet);
    }

    @Test
    public void testSelect() throws SQLException {
        String sql = "select * from t_order";
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            Long orderId = resultSet.getLong(1);
            Long userId = resultSet.getLong(2);
            String status = resultSet.getString(3);
            System.out.printf("order: orderId: %s, usereId: %s, status: %s%n", orderId, userId, status);
        }
    }

    @Test
    public void testPrimaryKeyUUID() {
        for (int i = 0; i < 100; i++) {
            UUID uuid1 = UUID.randomUUID();
            System.out.println(uuid1.toString());
        }
    }

    @Test
    public void testParse() {
        String sql = "select * from t_order where user_id = 1";
        String databaseType = "MySQL";
        ParseTree parseTree = new SQLParserEngine(databaseType).parse(sql, false);
        SQLVisitorEngine sqlVisitorStatementEngine = new SQLVisitorEngine(databaseType, "STATEMENT", new Properties());
        SQLStatement statement = sqlVisitorStatementEngine.visit(parseTree);

        SQLVisitorEngine formatEngine = new SQLVisitorEngine(databaseType, "FORMAT", new Properties());
        String formatSql = formatEngine.visit(parseTree);
        System.out.println(formatSql);

    }
}
