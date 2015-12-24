package us.wimsey.jdbc2kafka;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Properties;

/**
 * Created by dwimsey on 1/10/16.
 */
public class JDBCHelpers {
	public static Logger LOG = Logger.getLogger(JDBCReaderThread.class);

	private Properties m_Properties;
	public JDBCHelpers(Properties conf) {
		m_Properties = conf;
	}

	private Connection s_dbConnection = null;
	public void closeConnectionPool() {
		if(s_dbConnection != null) {
			try {
				s_dbConnection.close();
			} catch(Exception e) {

			}
			s_dbConnection = null;
		}
	}

	public Connection getConnection() throws SQLException {
		if(s_dbConnection == null) {
			s_dbConnection = DriverManager.getConnection(m_Properties.get("ConnectionString").toString());
		} else {
			if(s_dbConnection.isClosed() == true) {
				s_dbConnection = DriverManager.getConnection(m_Properties.get("ConnectionString").toString());
			}
		}
		return(s_dbConnection);
	}

	public Object executeScalerQuery(String queryString) throws SQLException {
		// setup the connection with the DB.
		if (s_dbConnection != null) {
			s_dbConnection = DriverManager.getConnection(m_Properties.get("ConnectionString").toString());
		}
		return(executeScalerQuery(s_dbConnection, queryString));
	}

	public Object executeScalerQuery(Connection dbConnection, String queryString) throws SQLException {

		Statement queryStatement = null;
		try {
			queryStatement = dbConnection.createStatement();
			ResultSet nrs = queryStatement.executeQuery(queryString);
			Object rVal = null;
			if(nrs.next() ) {
				rVal = nrs.getObject(1);
			} else {
				LOG.warn("Query returned no results!");
			}
			nrs.close();
			return(rVal);
		} finally {
			try {
				queryStatement.close();
			} catch(SQLException sqlExx ) {
				LOG.error ("Could not close JDBCHelpers connection for executeScalerQuery: " + sqlExx.toString());
			}
		}
	}

	public ResultSet executeQuery(String queryString) throws SQLException {
		if (s_dbConnection != null) {
			try {
				s_dbConnection.close();
			} catch (Exception e) {

			}
		}
		s_dbConnection = DriverManager.getConnection(m_Properties.get("ConnectionString").toString());
		return(executeQuery(s_dbConnection, queryString));
	}

	public ResultSet executeQuery(Connection dbConnection, String queryString) throws SQLException {
		return(dbConnection.createStatement().executeQuery(queryString));
	}

	public boolean execute(String queryString) throws SQLException {
		if (s_dbConnection != null) {
			try {
				s_dbConnection.close();
			} catch (Exception e) {

			}
		}
		s_dbConnection = DriverManager.getConnection(m_Properties.get("ConnectionString").toString());
		Statement s = s_dbConnection.createStatement();
		boolean rVal = s.execute(queryString);
		s.close();
		return(rVal);
	}
}
