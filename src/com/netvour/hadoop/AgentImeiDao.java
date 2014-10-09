package com.netvour.hadoop;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;


public class AgentImeiDao {

	 private static AgentImeiDao agentImeiDao = new AgentImeiDao();

	    private AgentImeiDao() {
	    }

	    public static AgentImeiDao getInstance() {
	        return agentImeiDao;
	    }

	    public void save(String imei){
	    	Connection conn = null;
	        try {
	            QueryRunner runner = new QueryRunner();
	            conn = DataSourceUtil.getConnection();
	            ScalarHandler<BigDecimal> handler = new ScalarHandler<BigDecimal>(1);
	            if(!has(conn, false, imei)) {
	            	runner.update(conn, "insert into agent_imei values(?)", imei);	
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
	        	try {
					DbUtils.close(conn);
				} catch (SQLException e) {
					e.printStackTrace();
				}
	        }
	    }
	    
	    public boolean has(String imei) throws SQLException {
			return has(DataSourceUtil.getConnection(), true, imei);
	    }
	    
	    private boolean has(Connection conn, boolean closeConn, String imei) {
	    	try {
	    		QueryRunner runner = new QueryRunner();
	            ScalarHandler<BigDecimal> handler = new ScalarHandler<BigDecimal>();
	            Long count = runner.query(conn, "select count(*) from agent_imei where imei=?", handler, imei).longValue();
	            return count > 0;
	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
	        	try {
					if (closeConn) DbUtils.close(conn);
				} catch (Exception e) {
				}
	        }
	    	 return false;
	    }
	    
	    public Set<String> all(){
	    	Connection conn = null;
	    	PreparedStatement pstmt = null;
	    	ResultSet rs = null;
	    	Set<String> result = new HashSet<String>();
	        try {
	        	conn = DataSourceUtil.getConnection();
	        	pstmt = conn.prepareStatement("select imei from agent_imei");
	        	rs = pstmt.executeQuery();
	        	while(rs.next()) {
	        		result.add(rs.getString("imei"));
	        	}
	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
				try {
					DbUtils.close(rs);
				}  catch(SQLException e){}
				try {
					DbUtils.close(pstmt);
				}  catch(SQLException e){}
				try {
					DbUtils.close(conn);
				}  catch(SQLException e){}
	        }
	        return result;
	    }

	    
	    public static void main(String[] args) {
//	    	Set<String> all = AgentImeiDao.getInstance().all();
//	    	for (String item : all) {
//				System.out.println(item);
//			}
	    	
	    	AgentImeiDao.getInstance().save("45699902175648100");
		}
	    
}
