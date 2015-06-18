package org.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class UvHiveJdbc {
	private static Connection conn = null;

	public static Connection getHiveConn() {
		if (conn == null) {
			try {
				System.out.println("=======");
				Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
				System.out.println("-------");
				conn = DriverManager.getConnection("jdbc:hive://127.0.0.1:10000/log",
						"", "");
				System.out.println("ok");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		return conn;
	}

	public static void closeHiveConn() throws SQLException {
		if (conn != null) {
			conn.close();
		}
	}

	public static void execDDL(String sql) throws SQLException {
		Connection conn = getHiveConn();
		Statement stmt = conn.createStatement();
		stmt.execute(sql);
	}

	public static ResultSet queryData(String sql) throws SQLException {
		Connection conn = getHiveConn();
		Statement stmt = conn.createStatement();
		ResultSet res = stmt.executeQuery(sql);
		return res;
	}

	public static void main(String[] args) {
		try {
			System.out.println("start");
			String sql = "create external table access(ip string,day string,url string,refurl string) row format delimited fields terminated by ',' location '/uvout/'";
			System.out.println(sql);
			execDDL(sql);
			sql = "create external table userip(ip string,username string) row format delimited fields terminated by ' ' location '/userlog/'";
			System.out.println(sql);
			execDDL(sql);
			sql = "select a.day,b.username,a.url,count(a.url) from access a join userip b on a.ip=b.ip group by b.username,a.url,a.day";
			System.out.println(sql);
			ResultSet res = queryData(sql);
			while (res.next()) {
				System.out.println(res.getString(1) + " " + res.getString(2)
						+ " " + res.getString(3) + " " + res.getString(4));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// 关闭Hive连接
				closeHiveConn();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
