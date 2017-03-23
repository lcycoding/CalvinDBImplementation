package netdb.software.benchmark.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MysqlService provides an easier way to access mysql database, execute queries
 * through the Mysql JDBC Driver.<br>
 * **Using step** <br>
 * 1. Connect to database by calling {@link #connect()}. It will return, if
 * successfully connected, a connection object.<br>
 * 2. Executing queries by calling {@link #executeQuery(String, Connection)} or
 * {@link #executeUpdateQuery(String, Connection)}.<br>
 * 3. Remember to {@link #disconnect(Connection)} after executing quries.<br>
 * 
 * @author pishaokan
 * 
 */

public class MysqlService {

	private static final String driver = "com.mysql.jdbc.Driver";
	// private static final String host = "jdbc:mysql://192.168.1.80";
	private static final String host = "jdbc:mysql://127.0.0.1";
	private static final String database = "/microbenchmark";
	private static final String username = "admin0";
	private static final String passwd = "netdb";
	private static Connection conn;
	static int requestCount = 0;

	static {
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(host + database, username,
					passwd);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Establishing the connection to the database.<br>
	 * The database host, user name, password are specified on above as static
	 * variables.<br>
	 * <i>**Remember to close the connection by calling {@link #disconnect()}
	 * after using it.**</i>
	 * 
	 * @return A connection object that has connected to host. Return null if
	 *         there are exceptions during the connection progress.
	 */
	public static Connection connect() {

		// Connection connToReutrn = null;
		// try {
		// requestCount++;
		//
		// connToReutrn = DriverManager.getConnection(host + database,
		// username, passwd);
		// } catch (SQLException ex) {
		//
		// ex.printStackTrace();
		// System.out.println("SQLException: " + ex.getMessage());
		// System.out.println("SQLState: " + ex.getSQLState());
		// System.out.println("VendorError: " + ex.getErrorCode());
		//
		// }
		//
		// return connToReutrn;

		return conn;
	}

	/**
	 * Disconnect to the host.
	 * 
	 * @param conn
	 *            The connection object to be disconnected.
	 */
	public static void disconnect(Connection conn) {
		// if (conn != null) {
		// try {
		// conn.close();
		// } catch (SQLException ex) {
		// ex.printStackTrace();
		// System.out.println("SQLException: " + ex.getMessage());
		// System.out.println("SQLState: " + ex.getSQLState());
		// System.out.println("VendorError: " + ex.getErrorCode());
		// }
		// }
	}

	/**
	 * Safely create a statement with passed connection.<br>
	 * <i>**Remember to close the returning Statement after using it.**</i>
	 * 
	 * @param conn
	 *            The connection used to create statement.
	 * @return The created statement. Null if there is exception when creating
	 *         statement.
	 */
	public static Statement createStatement(Connection conn) {

		Statement stm = null;

		try {
			stm = conn.createStatement();
		} catch (SQLException ex) {
			ex.printStackTrace();
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
		return stm;
	}

	/**
	 * Execute a query to the connected database.<br>
	 * <i>**Remember to close the returning ResultSet after using it.**</i>
	 * 
	 * @param query
	 *            The query to execute.
	 * @param statement
	 *            The statement for executing query.
	 * @return It will return the ResultSet object for further usage.<br>
	 *         If there is no result or the query is invalid, return null.
	 */
	public static ResultSet executeQuery(String query, Statement stm) {

		ResultSet resultSet = null;

		try {

			if (stm.execute(query)) {
				resultSet = stm.getResultSet();
			}

		} catch (SQLException ex) {

			ex.printStackTrace();
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

		}

		return resultSet;
	}

	/**
	 * Execute a update query to the connected database.<br>
	 * For example : CREATE, INSERT, UPDATE, DELETE.<br>
	 * <i>**Remember to close the returning ResultSet after using it.**</i>
	 * 
	 * @param sql
	 *            The query to execute.
	 * @param statement
	 *            The statement for executing query.
	 * @param conn
	 *            A connection object that has connected to host.
	 * @param autoGenerateKeys
	 *            Specify whether to request the auto generate key in case a
	 *            INSERT statement is made.
	 * @return Return the auto generate key. If the key is invalid, it will
	 *         return -1.
	 */
	public static int executeUpdateQuery(String sql, Statement stm,
			boolean autoGenerateKeys) {

		int key = -1;

		try {
			if (autoGenerateKeys) {
				stm.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
				ResultSet rs = stm.getGeneratedKeys();
				rs.first();
				key = rs.getInt(1);
				System.out.println(key);
			} else
				stm.executeUpdate(sql);
		} catch (SQLException ex) {

			ex.printStackTrace();
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

		}

		return key;

	}

	/**
	 * Execute a update query to the connected database.<br>
	 * For example : CREATE, INSERT, UPDATE, DELETE.<br>
	 * <i>**Remember to close the returning ResultSet after using it.**</i>
	 * 
	 * @param sql
	 *            The query to execute.
	 * @param statement
	 *            The statement for executing query.
	 * @param conn
	 *            A connection object that has connected to host.
	 */
	public static void executeUpdateQuery(String sql, Statement stm) {

		try {
			stm.executeUpdate(sql);
		} catch (SQLException ex) {

			ex.printStackTrace();
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

		}

	}

	public static int getMaxValue(String columnName, String tableName,
			String condition, Statement stm) {

		try {
			ResultSet rs = stm.executeQuery("SELECT MAX(`" + columnName
					+ "`) as `result` FROM `" + tableName + "` " + condition);
			if (rs.next())
				return rs.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}

	/**
	 * Safely close a statement.
	 * 
	 * @param stm
	 *            The statement to close.
	 */
	public static void closeStatement(Statement stm) {
		if (stm != null) {
			try {
				stm.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());
			}
		}
	}

	/**
	 * Safely close a result set.
	 * 
	 * @param rs
	 *            The result set to close.
	 */
	public static void closeResultSet(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());
			}
		}
	}
}
