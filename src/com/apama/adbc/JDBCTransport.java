package com.apama.adbc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Collections;
import com.softwareag.connectivity.AbstractSimpleTransport;
import com.softwareag.connectivity.Message;
import com.softwareag.connectivity.PluginConstructorParameters.TransportConstructorParameters;
import com.softwareag.connectivity.util.MapExtractor;
import java.sql.*;
import java.util.Map;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import java.util.ArrayList;
import java.lang.Thread;
import com.apama.util.concurrent.ApamaThread;

public class JDBCTransport extends AbstractSimpleTransport {
	InitialContext jndi;
	Connection jdbcConn;
	ApamaThread periodicCommitThread;

	final int batchSize = 500;
	final String jdbcURL;
	final String jndiName;

	final String username;
	final String password;
	final Hashtable<?,?> jndiEnvironment;
	final int maxRetries = 3;
	
	/** How often to automatically commit the connection. Set to 0 to disable. 
	 * This is usually more performant than the "autoCommit" JDBC feature which does it after every statement. */
	final float periodicCommitIntervalSecs; 

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JDBCTransport(org.slf4j.Logger logger, TransportConstructorParameters params) throws Exception 
	{
		super(logger, params);
		MapExtractor config = new MapExtractor(params.getConfig(), "config");


		// e.g. "org.sqlite.JDBC" Only needed for legacy older drivers. "Any JDBC 4.0 drivers that are found in your class path are automatically loaded"
		String driverClass = config.getStringDisallowEmpty("driverClass", null);
		if (driverClass != null)
			Class.forName(driverClass);

		jndiName = config.getStringDisallowEmpty("driverClass", null);
		jdbcURL = config.getStringDisallowEmpty("jdbcURL");
		if (!(jndiName == null ^ jdbcURL == null)) throw new IllegalArgumentException("Must set one of: jndiName, jdbcURL");

		username = config.getStringDisallowEmpty("username", null);
		password = config.getStringDisallowEmpty("password", "");

		periodicCommitIntervalSecs = config.get("periodicCommitIntervalSecs", 5.0f);
		
		jndiEnvironment = new Hashtable(config.getMap("jndiEnvironment", true).getUnderlyingMap());

		config.checkNoItemsRemaining();
	}

	private Connection createConnection() throws Exception
	{
		// TODO: automatic retry connection establishment (unless shutting down)
		
		if (jdbcURL != null) return DriverManager.getConnection(jdbcURL);
		
		// Preferred/modern approach is to get a DataSource instance from JNDI, which allows for connection pooling
		
		// TODO: check if we need to set thread context classloader (see Kafka plugin and also itrac, can't remember if we do this automatically or not)
		if (jndi == null)
			jndi = new InitialContext(jndiEnvironment);
		DataSource ds = (DataSource)jndi.lookup(jndiName);
		if (username != null) return ds.getConnection(username, password);
		return ds.getConnection();
	}
	
	public void start() throws Exception {
		jdbcConn = createConnection();
		jdbcConn.setAutoCommit(false);

		if (periodicCommitIntervalSecs >= 0)
			periodicCommitThread = new ApamaThread("JDBCTransport.periodicCommitThread") {
				@Override
				public void call() throws InterruptedException {
					while(true) {
						Thread.sleep((int)(periodicCommitIntervalSecs*1000));
						try {
							jdbcConn.commit();
						} catch (SQLException ex) { // TODO: think about error handling here
							logger.error("Failed to commit: ", ex);
						}
					}
				}
			}.startThread();

	}

	public void deliverMessageTowardsTransport(Message m) throws Exception {
		String eventType = ((String)m.getMetadataMap().get(Message.HOST_MESSAGE_TYPE)).substring("com.apama.adbc".length() + 1);
		
		@SuppressWarnings("rawtypes")
		MapExtractor payload = new MapExtractor((Map)m.getPayload(), "payload");

		// Unique id generic to all events from EPL, used in acknowledgements
		long messageId = payload.get("messageId", -1L);

		if (eventType.equals("Store")) {
			executeStore(messageId, payload);
		} else if (eventType.equals("Statement")) {
			executeStatement(payload, m, messageId);
		}
	}

	private void executeStatement(MapExtractor payload, Message m, long messageId) throws Exception{
		List<Message> msgList = new ArrayList<>();
		try {
			String sql_string = payload.getStringDisallowEmpty("sql"); 
			List<Object> parameters = payload.getList("parameters", Object.class, false);
			Statement stmt = null;
			boolean resultsAvailable;

			if(parameters.isEmpty()) {
				stmt = jdbcConn.createStatement();
				resultsAvailable = stmt.execute(sql_string);
			} else {
				PreparedStatement stmt_ = getPreparedStatement(sql_string);
				int i = 1;
				for(Object param : parameters) {
					stmt_.setObject(i, param);
					i++;
				}
				resultsAvailable = stmt_.execute();

				stmt = stmt_;
			}

			ResultSet rs = null;

			if(resultsAvailable) {
				rs = stmt.getResultSet();
			}

			int rowId = 0;
			// For each ResultSet
			while (rs != null) {
				// For each row
				ResultSetMetaData rsmd = rs.getMetaData();
				while (rs.next()) {
					Map <String, Object> rowMap = new HashMap<>();
					// For each column
					for(int i = 1; i <= rsmd.getColumnCount(); i++) {
						rowMap.put(rsmd.getColumnName(i), rs.getObject(i));
					}
					Map<String, Object> resultPayload = new HashMap<>();
					resultPayload.put("row", rowMap);
					resultPayload.put("messageId", messageId);
					resultPayload.put("rowId", rowId);
					Message resultMsg = new Message(resultPayload);
					resultMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.ResultSetRow");
					msgList.add(resultMsg);

					rowId = rowId + 1;
				}

				rs = stmt.getMoreResults() ? stmt.getResultSet() : null;
			}
			if (msgList.size() >0) {
				hostSide.sendBatchTowardsHost(msgList);
			}

			// Respond with StatementDone
			Map<String, Object> statementDonePayload = new HashMap<>();
			statementDonePayload.put("messageId", messageId);
			statementDonePayload.put("updateCount", stmt.getUpdateCount());
			Message statementDoneMsg = new Message(statementDonePayload);
			statementDoneMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.StatementDone");
			hostSide.sendBatchTowardsHost(Collections.singletonList(statementDoneMsg));
		} 
		catch (SQLTransientException e){
			//if this is a transient exception then we should retry it to see if it will just worked
			
			//if its 0 coming into this catch then uts the first times its failed
			if (maxRetries == 0){ 
				maxRetries = 3;
			else{
				maxRetries = maxRetries - 1;
			}

			String sql_string = payload.getStringDisallowEmpty("sql"); 
			List<Object> parameters = payload.getList("parameters", Object.class, false);
			// If maxRetries is 0 at this stage then all retries have been exhausted
			if (maxRetries > 0){
				logger.warn("Statement execution failed with a transient error and will now be retried - " + sql_string + " (" + parameters +")" );
				//TODO should we wait before retrying?
				executeStatement(payload, m, messageId);
			}
			else{
				logger.warn("Statement execution failed nd has been retried a maximum number of times - " + sql_string + " (" + parameters +")");
				// Throw exception and send statementDone/uery Done - back to use with the failure
			}
			
		}
		catch (SQLNonTransientException e){
			//If its a non transient exception then it wont 'just work' unless the cause is corrected
		}
		catch (SQLException ex) {
			/**
			String message = getSQLExceptionMessage(ex, "Error executing query");
			
			//Send QueryDone with errormsg
			Map<String, Object> queryDonePayload = new HashMap<>();
			queryDonePayload.put("messageId", messageId);
			queryDonePayload.put("errorMessage", message);
			queryDonePayload.put("eventCount", msgList.size());
			queryDonePayload.put("lastEventTime", lastEventTime);
			Message queryDoneMsg = new Message(queryDonePayload);
			queryDoneMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.QueryDone");

			hostSide.sendBatchTowardsHost(Collections.singletonList(queryDoneMsg));

			throw new Exception(message);//, db.isDisconnected(con));
			*/
		}
		finally {
			/**
			// clean up
			if (rs != null)	try	{rs.close();} catch(SQLException ex) {}
			if (stmt != null) try {stmt.close();} catch(SQLException ex) {}
			stmt = null;
			rs = null;
			*/
		}
	}

	private void executeStore(long messageId, MapExtractor payload) throws SQLException {
		String tableName = payload.getStringDisallowEmpty("tableName");
		ResultSet tableSchema = jdbcConn.getMetaData().getColumns(null, null, tableName, null);

		if(!tableSchema.next()) {
			String schema = "";
			// Create the table on demand
			for(Map.Entry<?, ?> i : payload.getMap("row", false).getUnderlyingMap().entrySet()) {
				if(!schema.isEmpty()) schema += ",";
				schema += i.getKey() + " ";
				if(i.getValue() instanceof Long) {
					schema += "INT";
				} else if(i.getValue() instanceof Double) {
					schema += "REAL";
				} else {
					schema += "TEXT";
				}
			}
			Statement stmtActual = jdbcConn.createStatement();
			stmtActual.executeUpdate("CREATE TABLE IF NOT EXISTS '" + tableName + "' (" + schema + ")");
			stmtActual.close();
			tableSchema = jdbcConn.getMetaData().getColumns(null, null, tableName, null);
			tableSchema.next();
		}

		ArrayList<Object> columnValues = new ArrayList<Object>();
		do {
			String columnName = tableSchema.getString(4);
			columnValues.add(payload.getMap("row", false).getStringDisallowEmpty(columnName));
		} while(tableSchema.next());

		String stmt = "INSERT INTO '" + tableName + "' VALUES(";
		for(int i = 0; i < columnValues.size(); i++) {
			stmt += "?";
			if(i < columnValues.size() - 1) stmt += ",";
		}
		stmt += ")";
		logger.info("stmt = " + stmt + " columnValues = " + columnValues.toString());
		PreparedStatement stmtActual = jdbcConn.prepareStatement(stmt);
		for(int i = 0; i < columnValues.size(); i++) {
			stmtActual.setObject(i + 1, columnValues.get(i));
		}
		stmtActual.execute();
		stmtActual.close();
	}

	public void sendSchemaEvent(Statement stmt, ResultSetMetaData rsmd, long messageId) throws SQLException
	{		
		// Simple schema only implemented - FieldOrdered and FieldTypes Fields
		Map<String, String> fieldTypes = new HashMap<>();
		List<String> fieldNamesOrdered = new ArrayList<>();

		// Find out how many columns in table
		int numColumns = rsmd.getColumnCount();
		
		for (int i = 0; i < numColumns; i++) {
			int column = i+1;
			String columnName = rsmd.getColumnName(column);
			fieldNamesOrdered.add(columnName);

			//int sqlType = rsmd.getColumnType(column);
			String sqlTypeName = rsmd.getColumnTypeName(column);
			fieldTypes.put(columnName, sqlTypeName);
			//int columnSize = rsmd.getColumnDisplaySize(column);
			//boolean autoIncrement = rsmd.isAutoIncrement(column);
			//int scale = rsmd.getScale(column);


			/*// Check for FLOAT/DOUBLE incorrectly reported as NUMBER
			if (db.isOracle) {
				String sqlTypeClassName = rsmd.getColumnClassName(column);
				if (sqlType == Types.NUMERIC && sqlTypeClassName.endsWith("Double")) {
					sqlType = Types.DOUBLE;
					sqlTypeName = "double";
				}
			}*/

		}
		
		if (fieldNamesOrdered.size() > 0){
			Map<String, Object> resultPayload = new HashMap<>();
			resultPayload.put("messageId", messageId);
			resultPayload.put("fieldOrder", fieldNamesOrdered);
			resultPayload.put("fieldTypes", fieldTypes);
			Message resultMsg = new Message(resultPayload);
			resultMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.ResultSchema");
			hostSide.sendBatchTowardsHost(Collections.singletonList(resultMsg));
		}
		else{
			//no schema - report error? throw exception have all of this in try
		}
	}

	public static String getSQLExceptionMessage(SQLException ex, String errorPrefix)
	{
		String error = errorPrefix + "; ";
		try {
			// Get the SQL state and error codes
			String sqlState = ex.getSQLState ();
			int errorCode = ex.getErrorCode();
			if ((sqlState != null && sqlState.length() > 0) || errorCode > 0) {
				error += "[" + sqlState + ":" + errorCode + "]";
			}
			// Append the error message or exception name
			if (ex.getMessage() == null) {
				error += ex.toString();
			}
			else {
				error += ex.getMessage();
			}

			// Add errors from any chained exceptions
			Exception exc = ex;
			Exception nextExc = null;
			while (true) {
				if (exc instanceof java.sql.SQLException) {
					if ((nextExc = ((SQLException)exc).getNextException()) != null) {
						
						// Get the SQL state and error codes
						sqlState = ((SQLException)nextExc).getSQLState ();
						errorCode = ((SQLException)nextExc).getErrorCode();
						if ((sqlState != null && sqlState.length() > 0) || errorCode > 0) {
							error += "; [" + sqlState + ":" + errorCode + "]";
						}
						else {
							error += "; ";
						}
						
						// Append the error message or exception name
						if (nextExc.getMessage() == null) {
							error += nextExc.toString();
						}
						else {
							error += nextExc.getMessage();
						}
						
						exc = nextExc;
					}
					else {
						// NULL exception, mo more
						break;
					}
				}
				else {
					// Not a SQLException
					error = getExceptionMessage(exc, error);
					break;
				}
			}
		}
		catch (Exception excp) {}
		return error;
	}

	public static String getExceptionMessage(Exception ex, String errorPrefix) // TODO: is this actually needed? Why not just log the stack trace for unexpected errors?
	{
		String error = errorPrefix + "; ";
		try {
			if (ex.getMessage() == null) {
				error += ex.toString();
			}
			else {
				error += ex.getMessage();
			}
			Throwable exc = ex;
			Throwable nextExc = null;
			// Add errors from any chained exceptions
			while ((nextExc = exc.getCause()) != null) {
				error += "; ";
				if (nextExc.getMessage() == null) {
					error += nextExc.toString();
				}
				else {
					error += nextExc.getMessage();
				}
				exc = nextExc;
			}
		}
		catch (Exception excp) {}
		return error;
	}

	/** Helper method that executes the specified lambda expression and logs a warning if it throws an exception. 
	 * 
	 * @param lambda e.g. <code>() -> foo.bar()</code>
	 * @param logMessagePrefix e.g. "Could not do thingummy: ".
	 */
	boolean tryElseWarn(CanThrow lambda, String logMessagePrefix)
	{
		try
		{
			lambda.run();
			return true;
		} catch (Exception e)
		{
			logger.warn(logMessagePrefix, e);
			return false;
		}
	}
	public interface CanThrow { void run() throws Exception; }
	
	public void shutdown() 
	{
		if (periodicCommitThread != null) 
			ApamaThread.cancelAndJoin(5000, true,  periodicCommitThread);
		
		if (jdbcConn != null) {
			tryElseWarn(() -> jdbcConn.commit(), "Could not perform a final commit on the JDBC connection: ");
			tryElseWarn(() -> jdbcConn.close(), "Could not close the JDBC connection: ");
		}
		if (jndi != null) 
			tryElseWarn(() -> jndi.close(), "Could not close JNDI context: ");
	}

	/**
	 * Creates a PreparedStatement on the current connection with the given SQL. Attempts to re-use previous PreparedStatements for the
	 * same SQL to save compilation expense.
	 */
	private PreparedStatement getPreparedStatement(String sql) throws SQLException {
		PreparedStatement ret = previousPreparedStatements.get(sql);
		if(ret == null) {
			ret = jdbcConn.prepareStatement(sql);
			previousPreparedStatements.put(sql, ret);
		}
		return ret;
	}

	/** SQL string to statement map, supports getPreparedStatement's caching */
	private Map<String, PreparedStatement> previousPreparedStatements = new HashMap<String, PreparedStatement>();
}
