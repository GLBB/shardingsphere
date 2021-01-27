package cn.gl.divede_database;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.val;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.keygen.KeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * 分库测试
 * 环境准备: 创建数据库
 * drop schema if exists ds0;
 * drop schema if exists ds1;
 * create schema if not exists ds0;
 * create schema if not exists ds1;
 *
 * t_order, t_order_item 分库, id 使用雪花算法
 * t_address 广播表
 *
 * CREATE TABLE IF NOT EXISTS t_order (
 *     order_id BIGINT NOT NULL AUTO_INCREMENT,
 *     user_id INT NOT NULL,
 *     address_id BIGINT NOT NULL,
 *     status VARCHAR(50),
 *     PRIMARY KEY (order_id)
 * )
 *
 * CREATE TABLE IF NOT EXISTS t_order_item(
 *     order_item_id BIGINT NOT NULL AUTO_INCREMENT,
 *     order_id BIGINT NOT NULL,
 *     user_id INT NOT NULL,
 *     status VARCHAR(50),
 *     PRIMARY KEY (order_item_id)
 * )
 *
 * CREATE TABLE IF NOT EXISTS t_address (
 *     address_id BIGINT NOT NULL,
 *     address_name VARCHAR(100) NOT NULL,
 *     PRIMARY KEY (address_id)
 * )
 */
public class DivideDatabaseTest {

    DataSource dataSource;

    /**
     * 初始化数据源
     */
    @Before
    public void init() throws SQLException {
        val shardingRuleConf = new ShardingRuleConfiguration();
        // 1. t_order , 指定id 生成算法
        val orderTableShardingConf = new ShardingTableRuleConfiguration("t_order");
        val orderIdGenStrategy = new KeyGenerateStrategyConfiguration("order_id", "idGenerateKey");
        orderTableShardingConf.setKeyGenerateStrategy(orderIdGenStrategy);
        shardingRuleConf.getTables().add(orderTableShardingConf);
        // 2. t_order_item
        val itemShardingConf = new ShardingTableRuleConfiguration("t_order_item");
        val itemIdGenStrategy = new KeyGenerateStrategyConfiguration("order_item_id", "idGenerateKey");
        itemShardingConf.setKeyGenerateStrategy(itemIdGenStrategy);
        shardingRuleConf.getTables().add(itemShardingConf);
        // 3. t_address 广播表
        shardingRuleConf.getBroadcastTables().add("t_address");
        // 4.1 设置默认分库字段 user_id
        val shardingDbConf = new StandardShardingStrategyConfiguration("user_id", "dbShardingKey");
        shardingRuleConf.setDefaultDatabaseShardingStrategy(shardingDbConf);
        // 5. 填充 id 生成 key
        Properties idGenProps = new Properties();
        idGenProps.setProperty("worker-id", "123");
        val idGeAlgorithmConf = new ShardingSphereAlgorithmConfiguration("SNOWFLAKE", idGenProps);
        shardingRuleConf.getKeyGenerators().put("idGenerateKey", idGeAlgorithmConf);
        // 6. 填充分库 key
        Properties dbShardingProps = new Properties();
        dbShardingProps.put("algorithm-expression", "ds${user_id % 2}");
        val dbShardingAlgorithmConf = new ShardingSphereAlgorithmConfiguration("INLINE", dbShardingProps);
        shardingRuleConf.getShardingAlgorithms().put("dbShardingKey", dbShardingAlgorithmConf);
        // 7. datasource
        val ds0 = createDataSource("127.0.0.1", "3306", "gl", "123", "ds0");
        val ds1 = createDataSource("127.0.0.1", "3306", "gl", "123", "ds1");
        HashMap<String, DataSource> dataSourceMap = new HashMap<>();
        dataSourceMap.put("ds0", ds0);
        dataSourceMap.put("ds1", ds1);
        dataSource = ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, Collections.singleton(shardingRuleConf), new Properties());
    }

    private DataSource createDataSource(String host, String port, String user, String pwd, String dataSourceName) {
        HikariDataSource result = new HikariDataSource();
        result.setDriverClassName("com.mysql.jdbc.Driver");
        result.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=UTF-8", host, port, dataSourceName));
        result.setUsername(user);
        result.setPassword(pwd);
        return result;
    }

    @Test
    public void testCreateOrder() throws SQLException {
        String createOrderSql = "CREATE TABLE IF NOT EXISTS t_order (\n" +
            "    order_id BIGINT NOT NULL AUTO_INCREMENT, \n" +
            "    user_id INT NOT NULL, \n" +
            "    address_id BIGINT NOT NULL, \n" +
            "    status VARCHAR(50), \n" +
            "    PRIMARY KEY (order_id)\n" +
            ")";
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        int res = statement.executeUpdate(createOrderSql);
        System.out.println(res);
    }

    @Test
    public void testCreateItem() throws SQLException {
        String createItemSql = "CREATE TABLE IF NOT EXISTS t_order_item(\n" +
            "    order_item_id BIGINT NOT NULL AUTO_INCREMENT, \n" +
            "    order_id BIGINT NOT NULL, \n" +
            "    user_id INT NOT NULL, \n" +
            "    status VARCHAR(50), \n" +
            "    PRIMARY KEY (order_item_id)\n" +
            ")";
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        int res = statement.executeUpdate(createItemSql);
        System.out.println(res);
    }

    @Test
    public void testCreateAddress() throws SQLException {
        String createAddressSql = "CREATE TABLE IF NOT EXISTS t_address (\n" +
            "    address_id BIGINT NOT NULL, \n" +
            "    address_name VARCHAR(100) NOT NULL, \n" +
            "    PRIMARY KEY (address_id)\n" +
            ")";
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        int res = statement.executeUpdate(createAddressSql);
        System.out.println(res);
    }




}
