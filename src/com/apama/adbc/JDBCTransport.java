package com.apama.adbc;

import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import com.softwareag.connectivity.AbstractSimpleTransport;
import com.softwareag.connectivity.Message;
import com.softwareag.connectivity.PluginConstructorParameters.TransportConstructorParameters;
import com.softwareag.connectivity.util.MapExtractor;
import java.sql.*;
import java.util.Map;
import java.util.ArrayList;
import java.lang.Thread;
import com.apama.util.concurrent.ApamaThread;

public class JDBCTransport extends AbstractSimpleTransport {
	String jdbcURL;
	Connection jdbcConn;
	ApamaThread autoCommitThread;
	Integer batchSize = 500;

	public JDBCTransport(org.slf4j.Logger logger, TransportConstructorParameters params) throws Exception 
	{
		super(logger, params);
		MapExtractor config = new MapExtractor(params.getConfig(), "config");
		jdbcURL = config.getStringDisallowEmpty("jdbcURL");
		config.checkNoItemsRemaining();
	}

	public void start() throws Exception {
		Class.forName("org.sqlite.JDBC");
		jdbcConn = DriverManager.getConnection(jdbcURL);
		jdbcConn.setAutoCommit(false);

		autoCommitThread = new ApamaThread("JDBCTransport.autoCommitThread") {
			@Override
			public void call() {
				try {
					while(true) {
						Thread.sleep(5000);
						try {
							jdbcConn.commit();
						} catch (SQLException s) {
						}
					}
				} catch (java.lang.InterruptedException e) {
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

	public void executeQuery(MapExtractor payload, Message m, long messageId) throws Exception{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String errorPrefix = "Error executing query";
		List<Message> msgList = new ArrayList<>();
		long lastEventTime = 0;
		Integer rowId = 0;
		
		try {
			String queryString = payload.getStringDisallowEmpty("query"); 
			logger.info("QUERY: " + queryString);
			stmt = jdbcConn.prepareStatement(queryString);
			logger.info("Executing query '" + queryString + "'");

			// Execute the query
			boolean resultsAvailable = true;
			rs = stmt.executeQuery();

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
						columnValue = rs.getString(i);		
						logger.info("ROW: FieldName: " + rsColumnName + " Value: " + columnValue);				
	
						if (rsColumnName == null || rsColumnName.length() == 0) {
							logger.warn("Column #" +  i + " name is NULL, skipping");
							break;
						}
	
						if (columnValue != null) {
							rowMap.put(rsColumnName,  columnValue);
						}
						// Do nothing for NULL values, they are not added to the normalized event
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
				lastEventTime = System.currentTimeMillis();	
				// Send the result event(s) to the Host
				hostSide.sendBatchTowardsHost(msgList);
			}
			else{
				//no results
			}

			//send ack even if no results - QueryDone
			Map<String, Object> queryDonePayload = new HashMap<>();
			queryDonePayload.put("messageId", messageId);
			queryDonePayload.put("errorMessage", "");
			queryDonePayload.put("eventCount", msgList.size());
			queryDonePayload.put("lastEventTime", lastEventTime);
			Message queryDoneMsg = new Message(queryDonePayload);
			queryDoneMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.QueryDone");

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
			Map<String, Object> queryDonePayload = new HashMap<>();
			queryDonePayload.put("messageId", messageId);
			queryDonePayload.put("errorMessage", message);
			queryDonePayload.put("eventCount", msgList.size());
			queryDonePayload.put("lastEventTime", lastEventTime);
			Message queryDoneMsg = new Message(queryDonePayload);
			queryDoneMsg.putMetadataValue(Message.HOST_MESSAGE_TYPE, "com.apama.adbc.QueryDone");

			hostSide.sendBatchTowardsHost(Collections.singletonList(queryDoneMsg));

			throw new Exception(message);//, db.isDisconnected(con));
		}
		catch (Exception excp) {
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
		}
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

	public static String getExceptionMessage(Exception ex, String errorPrefix)
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


	public void shutdown() throws SQLException {
		if (autoCommitThread != null) autoCommitThread.interrupt();
		if (jdbcConn != null) {
			jdbcConn.commit();
			jdbcConn.close();
		}
	}
}
