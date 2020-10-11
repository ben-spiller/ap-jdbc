package com.apama.adbc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Collections;
import java.util.Properties;
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

/**
 * Each instance of this transport is single-threaded (TODO: check this is always followed)
 */
public class JDBCTransport extends AbstractSimpleTransport {
	InitialContext jndi;
	Connection jdbcConn;
	ApamaThread periodicCommitThread;

	final int batchSize = 500;
	final String jdbcURL;
	final String jdbcUser;
	final String jdbcPassword;
	final String jndiName;

	final Hashtable<?,?> jndiEnvironment;
	final int maxRetries = 3;
	
	/** How often to automatically commit the connection. Set to 0 to disable. 
	 * This is usually more performant than the "autoCommit" JDBC feature which does it after every statement. */
	final float periodicCommitIntervalSecs; 
	/** The JDBC autoCommit feature */
	final boolean autoCommit; 

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

		jdbcUser = config.getStringDisallowEmpty("jdbcUser", null);
		jdbcPassword = config.getStringDisallowEmpty("jdbcPassword", "");

		autoCommit = config.get("autoCommit", true);
		periodicCommitIntervalSecs = config.get("periodicCommitIntervalSecs", 0);
		if (autoCommit && periodicCommitIntervalSecs>0) throw new IllegalArgumentException("Cannot enable both autoCommit and periodicCommitIntervalSecs");
		
		jndiEnvironment = new Hashtable(config.getMap("jndiEnvironment", true).getUnderlyingMap());

		config.checkNoItemsRemaining();
	}

	private Connection createConnection() throws Exception
	{
		// TODO: automatic retry connection establishment (unless shutting down)
		Connection returnConn;
		Properties sqlprops = new Properties();
		//username and password properties may be different for different databases
		if (jdbcUser != null){
			sqlprops.setProperty("user",jdbcUser);
			sqlprops.setProperty("password", jdbcPassword);
		}
		if (jdbcURL != null) returnConn = DriverManager.getConnection(jdbcURL, sqlprops);
		
		// Preferred/modern approach is to get a DataSource instance from JNDI, which allows for connection pooling
		
		// TODO: check if we need to set thread context classloader (see Kafka plugin and also itrac, can't remember if we do this automatically or not)
		else {
			if (jndi == null)
				jndi = new InitialContext(jndiEnvironment);
			DataSource ds = (DataSource)jndi.lookup(jndiName);
			if (jdbcUser != null) {
				returnConn = ds.getConnection(jdbcUser, jdbcPassword);
			}else{
				returnConn = ds.getConnection();
			}
		}
		return returnConn;
	}
	
	public void start() throws Exception {
		jdbcConn = createConnection();
		jdbcConn.setAutoCommit(autoCommit);
		logger.info("Connected to JDBC "+jdbcConn+" with autoCommit="+autoCommit);
		//make sure the connection is valid
		boolean goodConn = jdbcConn.isValid(5); //will throw if this fails

		if (periodicCommitIntervalSecs > 0)
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
		} else if (eventType.equals("SQLStatement")) {
			executeStatement(payload, m, messageId, false, -1);
		} else if (eventType.equals("SmallStatement")) {
			boolean smallResultSet = true;
			long maxSmallQuerySize = payload.get("maxSmallQuerySize", -1L);
			logger.info("Received execute SmallStatement smallResultSet " + smallResultSet + " maxSmallQuerySize " + maxSmallQuerySize);
			payload = new MapExtractor((Map)m.getPayload(), "payload.statement");
			executeStatement(payload, m, messageId, smallResultSet, maxSmallQuerySize);
		} else {
			throw new RuntimeException("Unsupported event type: "+eventType);
		}
	}

	
	private Statement configureStatement(MapExtractor payload, Statement statement) throws SQLException
	{
		long maxRows = payload.get("maxRows", 0L);
		if (maxRows > 0)
		{
			if (maxRows > Integer.MAX_VALUE)
				statement.setLargeMaxRows(maxRows); // not supported by all databases
			else
				statement.setMaxRows((int)maxRows);
		}
		
		long queryTimeoutSecs = payload.get("queryTimeoutSecs", 0L);
		if (queryTimeoutSecs > 0) statement.setQueryTimeout((int)queryTimeoutSecs);
		
		return statement;
	}
	
	// This method assumes it's not called concurrently by more than one thread
	private void executeStatement(MapExtractor payload, Message m, long messageId, boolean smallResultSet, long maxSmallQuerySize) 
	{
		Map<String, Object> statementDonePayload = new HashMap<>();
		statementDonePayload.put("messageId", messageId);
		statementDonePayload.put("error", "");
		statementDonePayload.put("updateCount", 0);
		Message statementDoneMsg = new Message(statementDonePayload);
		statementDoneMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.SQLStatementDone");

		
		Statement stmt = null;
		ResultSet rs = null;
		try {
			smallResultSet = smallResultSet || payload.get("resultsInOneEvent", false); // TODO: if we use the new ExecuteStatement event (with maxRows) no need for smallResultSet/maxSmallQuerySize to be passed in separately 
			
			//TODO this works find for normal Statments - for Small statments sql and parameters are at the wrong level 
			// in the payload to read them with the following, either the payload passed in needs to change or reading them for SmallStataments does.
			List<String> sql = payload.getList("sql", String.class, false); 
			List<MapExtractor> parameterBatches = payload.getListOfMaps("parameterBatches", true);

			if (parameterBatches.isEmpty()) // non-prepared statement
			{
				stmt = configureStatement(payload, jdbcConn.createStatement());
				
				if (sql.size() <= 1) {
					stmt.execute(sql.get(0));
				} else { // batch of SQL updates commands - can't be used for queries
					for (String s: sql) 
						stmt.addBatch(s);
					stmt.executeBatch();
				}
			} 
			else // prepared statement with parameters
			{
				if (sql.size() != 1) throw new IllegalArgumentException("If providing SQL parameters there must be exactly one SQL statement string but "+sql.size()+" were provided");
				
				PreparedStatement pstmt = getPreparedStatement(sql.get(0));
				stmt = configureStatement(payload, pstmt);
				for(MapExtractor params : parameterBatches) {
					for (Map.Entry<?,?> param: params.getUnderlyingMap().entrySet()) {
						// TODO: "Not all databases allow for a non-typed Null to be sent to the backend. For maximum portability, the setNull or the setObject(int parameterIndex, Object x, int sqlType) method should be used instead of setObject(int parameterIndex, Object x)."
						logger.info("setObject: "+(int)(long)(Long)param.getKey()+", "+param.getValue());
						pstmt.setObject((int)(long)(Long)param.getKey(), param.getValue()); // TODO: does this work for all valid SQL types e.g. XML, blob, etc?
					}
					pstmt.execute();
				}
			}

			rs = stmt.getResultSet(); // returns null if None

			List<Message> msgList = new ArrayList<>();
			List<Map <String, Object>> rowList = new ArrayList<>();

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
					
					if (smallResultSet == true){
						rowList.add(rowMap);
						if (rowList.size() >= maxSmallQuerySize){ // TODO: probably this check isn't needed due to setMaxRows()?

							//send back a new event containing all rows
							Map<String, Object> smallResultPayload = new HashMap<>();
							smallResultPayload.put("messageId", messageId);
							smallResultPayload.put("rows", rowList);
							Message smallResultMsg = new Message(smallResultPayload);
							smallResultMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.SmallResultsSet");
							msgList.add(smallResultMsg);
							hostSide.sendBatchTowardsHost(msgList);
							
							rowList.clear();
							msgList.clear();
							//TODO if results are still available, stop the query and ignore any other incoming reults;
							//...
						}
					}
					else{
						Map<String, Object> resultPayload = new HashMap<>();
						resultPayload.put("row", rowMap);
						resultPayload.put("messageId", messageId);
						resultPayload.put("rowId", rowId);
						Message resultMsg = new Message(resultPayload);
						resultMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.ResultSetRow");
						msgList.add(resultMsg);
						if (msgList.size() >= batchSize){
							//TODO addin back in btching ability, need to confirm with Richard if this was deliberately removed, also lastEventTime
							//lastEventTime = System.currentTimeMillis();	
							// Send the result event(s) to the Host
							hostSide.sendBatchTowardsHost(msgList);
							msgList.clear();
						}		
					}
					rowId = rowId + 1;
				}

				rs.close();
				rs = stmt.getMoreResults() ? stmt.getResultSet() : null;
			}
			
			if (smallResultSet == true && rowList.size() >= maxSmallQuerySize){
				//send back a new event containing all rows
			Map<String, Object> smallResultPayload = new HashMap<>();
				smallResultPayload.put("messageId", messageId);
				smallResultPayload.put("rows", rowList);
				Message smallResultMsg = new Message(smallResultPayload);
				smallResultMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.SmallResultsSet");
				msgList.add(smallResultMsg);
				hostSide.sendBatchTowardsHost(msgList);
			}
			else if (msgList.size() >0) {
				hostSide.sendBatchTowardsHost(msgList);
			}

			// Respond with StatementDone
			statementDonePayload.put("updateCount", stmt.getUpdateCount());
			hostSide.sendBatchTowardsHost(Collections.singletonList(statementDoneMsg));
		} 
		/*
		catch (SQLTransientException e){
			//if this is a transient exception then we should retry it to see if it will just worked
			
			//if currentRetryCount is 0 coming into this catch then uts the first times its failed
			if (currentRetryCount == 0){ 
				currentRetryCount = maxRetries;
			}
			else{
				currentRetryCount = currentRetryCount - 1;
			}

			String sql_string = payload.getStringDisallowEmpty("sql"); 
			List<Object> parameters = payload.getList("parameters", Object.class, false);
			// If currentRetryCount is 0 at this stage then all retries have been exhausted
			if (currentRetryCount > 0){
				logger.warn("Statement execution failed with a transient error and will now be retried - " + sql_string + " (" + parameters +")" );
				//TODO should we wait before retrying?
				executeStatement(payload, m, messageId, currentRetryCount);
			}
			else{
				logger.warn("Statement execution failed nd has been retried a maximum number of times - " + sql_string + " (" + parameters +")");
				// Throw exception and send statementDone/uery Done - back to use with the failure
			}
			
		}
		catch (SQLNonTransientException e){
			//If its a non transient exception then it wont 'just work' unless the cause is corrected
		}*/
		catch (SQLException ex) {
			logger.warn("Failed to execute query due to: "+ex+" - query is "+payload+": "); // TODO: remove later, but useful during dev
			String message = getSQLExceptionMessage(ex, "Error executing query");

			statementDonePayload.put("error", message);
			hostSide.sendBatchTowardsHost(Collections.singletonList(statementDoneMsg));

			throw new RuntimeException(message, ex);//, db.isDisconnected(con));
		} catch (RuntimeException e)
		{
			// TODO: better error handling here too
			statementDonePayload.put("error", e.toString());
			hostSide.sendBatchTowardsHost(Collections.singletonList(statementDoneMsg));
			logger.warn("Failed to execute query "+payload+": ", e);
		}
		finally {
			// TODO: maybe this is the best place to put the sendBatchTowardsHost, to ensure it always gets sent
			
			tryElseWarn(rs,  x->x.close(), "Could not close ResultSet object for "+stmt+": ");
			if (!(stmt instanceof PreparedStatement)) 
				tryElseWarn(stmt, x -> x.close(), "Could not close statement object "+stmt+": ");
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
			// TODO: SQL injection attack here
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

	/** Helper method that executes the specified lambda expression to close a resource and logs a warning if it throws an exception. 
	 * 
	 * @param lambda e.g. <code>() -> foo.bar()</code>
	 * @param logMessagePrefix e.g. "Could not do thingummy: ".
	 */
	<T> boolean tryElseWarn(T obj, CanThrowWithParam<T> lambda, String logMessagePrefix)
	{
		if (obj == null) return true;
		try
		{
			lambda.run(obj);
			return true;
		} catch (Exception e)
		{
			logger.warn(logMessagePrefix, e);
			return false;
		}
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
	public interface CanThrowWithParam<T> { void run(T obj) throws Exception; }
	
	public void shutdown() 
	{
		if (periodicCommitThread != null) 
			ApamaThread.cancelAndJoin(5000, true,  periodicCommitThread);
		
		if (jdbcConn != null) {
			if (periodicCommitThread != null && !autoCommit)
				tryElseWarn(() -> jdbcConn.commit(), "Could not perform a final commit on the JDBC connection: ");
			
			tryElseWarn(() -> jdbcConn.close(), "Could not close the JDBC connection: ");
		}
		tryElseWarn(jndi, x -> x.close(), "Could not close JNDI context: ");
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
		// TODO: use a java class that is a LRU cache and close() prepared statements when evicted 
		return ret;
	}

	/** SQL string to statement map, supports getPreparedStatement's caching */
	private Map<String, PreparedStatement> previousPreparedStatements = new HashMap<String, PreparedStatement>();
}
