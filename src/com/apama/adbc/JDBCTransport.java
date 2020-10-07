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

		if (eventType.equals("StartQuery")){
			executeQuery(payload, m, messageId);
		} else if (eventType.equals("Command")) {
			executeCommand(messageId, payload);
		} else if (eventType.equals("Store")) {
			executeStore(messageId, payload);
		} else if (eventType.equals("Statement")) {
			executeStatement(payload, m, messageId);
		}
	}

	public void executeStatement(MapExtractor payload, Message m, long messageId) throws Exception{
		String errorPrefix = "Error executing query";
		List<Message> msgList = new ArrayList<>();
		
		try {
			String sql_string = payload.getStringDisallowEmpty("query"); 
			logger.info("STATEMENT: " + sql_string);
			PreparedStatement stmt = jdbcConn.prepareStatement(sql_string);

			boolean resultsAvailable = stmt.execute();
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
					for(int i = 0; i <= rsmd.getColumnCount(); i++) {
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

				if(stmt.getMoreResults()) {
					rs = stmt.getResultSet();
				}
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
		} catch (SQLException ex) {
			/**
			String message = getSQLExceptionMessage(ex, errorPrefix);
			
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

	private void executeCommand(long messageId, MapExtractor payload) throws SQLException {
		String operation = payload.getStringDisallowEmpty("operationString");
		Statement stmt = jdbcConn.createStatement();
		stmt.executeUpdate(operation);
		stmt.close();

		Message ack = new Message(Collections.singletonMap("messageId", messageId),
		                          Collections.singletonMap(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.CommandAck"));
		hostSide.sendBatchTowardsHost(Collections.singletonList(ack));
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

	public void executeQuery(MapExtractor payload, Message m, long messageId) throws Exception{ // TODO: this method is too long; refactor into smaller methods
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String errorPrefix = "Error executing query";
		List<Message> msgList = new ArrayList<>();
		long lastEventTime = 0;
		Integer rowId = 0;
		
		Map<String, Object> queryDonePayload = new HashMap<>();
		queryDonePayload.put("messageId", messageId);
		//queryDonePayload.put("lastEventTime", lastEventTime); // 
		Message queryDoneMsg = new Message(queryDonePayload);
		queryDoneMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.QueryDone");

		
		try {
			String queryString = payload.getStringDisallowEmpty("query"); 
			logger.info("QUERY: " + queryString);
			stmt = jdbcConn.prepareStatement(queryString);
			logger.info("Executing query '" + queryString + "'");

			// Execute the query
			boolean resultsAvailable = true;
			rs = stmt.executeQuery(); // TODO: cope with non-query statements that return multiple ResultSet's; also shoudl use try-with-using here

			//AbstractRelationalDatabase.SchemaAttribute[] info = null;
			ResultSetMetaData rsmd = null;
			ParameterMetaData pmd = null;
			int numColumns = 0;
			int schemaId = 1;			
			
			while (resultsAvailable) {
				if (rs != null) {
					rsmd = rs.getMetaData();
					numColumns = rsmd.getColumnCount();
				}				
			
				sendSchemaEvent(stmt, rsmd, messageId);

				// Fetch the data and send the events to the decoder
				while (rs.next()) {
					Map <String, Object> rowMap = new HashMap<>();

					logger.info("After get RS.next" + queryString + "'");
					// Add schema id to result event
					//addSchemaId(schemaId);
	
					// Add column name:value pairs to the result event
					int mode = ParameterMetaData.parameterModeUnknown;
					String rsColumnName;
					String columnName;
					String columnValue;
					int columnNameSuffix = 1;
					for (int i=1; i<=numColumns; i++) { 
						rsColumnName = rsmd.getColumnName(i);
						//columnName = info[i-1].getName();
						columnValue = rs.getString(i); // TODO: what about other types?
						logger.info("ROW: FieldName: " + rsColumnName + " Value: " + columnValue);				
	
						if (rsColumnName == null || rsColumnName.length() == 0) {
							logger.warn("Column #" +  i + " name is NULL, skipping");
							break;
						}
	
						if (columnValue != null) {
							rowMap.put(rsColumnName,  columnValue);
						}
						// Do nothing for NULL values, they are not added to the payload
					}
	
					//accumulate a batch of events and send back to the host when the batch is full.
					if (rowMap.size() > 0){
						Map<String, Object> resultPayload = new HashMap<>();
						resultPayload.put("row", rowMap);
						resultPayload.put("messageId", messageId);
						rowId = rowId + 1;
						//logger.info("For query id " + messageId + " Send row Id " + rowId);
						resultPayload.put("rowId", rowId);
						Message resultMsg = new Message(resultPayload);
						resultMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.ResultEvent");
						msgList.add(resultMsg);
						if (msgList.size() >= batchSize){
							lastEventTime = System.currentTimeMillis();	
							// Send the result event(s) to the Host
							hostSide.sendBatchTowardsHost(msgList);
							msgList.clear();
						}
					}
				}
								
				// Get the next result set
				resultsAvailable = stmt.getMoreResults();
				if (resultsAvailable) {
					rs = stmt.getResultSet();
					schemaId++;
				}
			}
			//Send any remaining resultEvents to the host.
			if (msgList.size() >0){		
				lastEventTime = System.currentTimeMillis();	// TODO: why would this timestamp be useful?
				// Send the result event(s) to the Host
				hostSide.sendBatchTowardsHost(msgList);
			}
			else{
				//no results
			}

			//send ack even if no results - QueryDone
			queryDonePayload.put("errorMessage", "");
			queryDonePayload.put("eventCount", msgList.size());
			queryDonePayload.put("lastEventTime", lastEventTime);
			hostSide.sendBatchTowardsHost(Collections.singletonList(queryDoneMsg));

			
		}
		//TODO in future need to handle connection errors here (as well as other places) and retry to reopen the connection if its not open
		/*catch (DataSourceException dex) {
			//String message = errorPrefix + ", " + dex.getMessage();
			//throw new DataSourceException(message, dex.isDisconnected());
		}*/
		catch (SQLException ex) {
			String message = getSQLExceptionMessage(ex, errorPrefix);
			
			//Send QueryDone with errormsg
			queryDonePayload.put("errorMessage", message);
			queryDonePayload.put("eventCount", msgList.size());
			hostSide.sendBatchTowardsHost(Collections.singletonList(queryDoneMsg));

			throw new Exception(message);//, db.isDisconnected(con));
		}
		/*catch (Exception excp) { // is there really any other kind of exception we need to catch here?
			String message = getExceptionMessage(excp, errorPrefix);
			logger.debug(errorPrefix, excp);

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
		}*/
		finally {
			// clean up
			if (rs != null)	try	{rs.close();} catch(SQLException ex) {}
			if (stmt != null) try {stmt.close();} catch(SQLException ex) {}
			stmt = null;
			rs = null;
		}
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
}
