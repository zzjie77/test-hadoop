package com.netvour.hadoop;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * User: zhangjunjie
 * Date: 14-6-9
 * Time: 下午12:17
 */
public class DataSourceUtil {

    private static DruidDataSource dataSource = new DruidDataSource();

    static {
        try {
            Properties dsProp = PropertiesUtil.getDataSourceProp();

            //基本属性 url、user、password
            dataSource.setUrl(dsProp.getProperty("jdbc_url"));
            dataSource.setUsername(dsProp.getProperty("jdbc_username"));
            dataSource.setPassword(dsProp.getProperty("jdbc_password"));

            //配置初始化大小、最小、最大
            dataSource.setInitialSize(Integer.parseInt(dsProp.getProperty("initialSize")));
            dataSource.setMinIdle(Integer.parseInt(dsProp.getProperty("initialSize")));
            dataSource.setMaxActive(Integer.parseInt(dsProp.getProperty("initialSize")));

            //配置获取连接等待超时的时间
            dataSource.setMaxWait(Integer.parseInt(dsProp.getProperty("initialSize")));

            //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
            dataSource.setTimeBetweenEvictionRunsMillis(60000);

            //配置一个连接在池中最小生存的时间，单位是毫秒
            dataSource.setMinEvictableIdleTimeMillis(300000);

            dataSource.setValidationQuery("select 1 from dual");
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnBorrow(false);
            dataSource.setTestOnReturn(false);

            //打开PSCache，并且指定每个连接上PSCache的大小
            dataSource.setPoolPreparedStatements(true);
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);

            //配置监控统计拦截的filters
            dataSource.setFilters("stat");

            dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static DataSource getDataSource() {
        return dataSource;
    }
    
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void main(String[] args) throws Exception {
        DataSource ds = DataSourceUtil.getDataSource();
        Connection conn = ds.getConnection();
        PreparedStatement psmt = conn.prepareStatement("select t.* from SYS_DICT t");
        ResultSet rs = psmt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("LABEL") + "=" + rs.getString("VALUE"));
        }
        rs.close();
        psmt.close();
        conn.close();
        
    	
//        QueryRunner runner = new QueryRunner(DataSourceUtil.getDataSource());
//        String sql = "insert into agent_imei values(?)";
//        DecimalFormat df = new DecimalFormat("00");
//        for(int i=0; i<100; i++){
//        	System.out.println(df.format(i));
//        	runner.update(sql, "456999021756480"+df.format(i));
//        }
        
    }

}
