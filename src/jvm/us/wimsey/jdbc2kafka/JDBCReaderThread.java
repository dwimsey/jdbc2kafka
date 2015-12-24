/**
 * Copyright 2015 Bandwidth, Inc - All rights reserved.
 * Author: David Wimsey <dwimsey@bandwidth.com>
 */
package us.wimsey.jdbc2kafka;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import java.sql.Statement;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.json.JSONObject;


public class JDBCReaderThread extends Thread {
	public static Logger LOG = Logger.getLogger(JDBCReaderThread.class);

	private boolean _isDistributed;
	private boolean _keepGoing = true;

	// Configuration options
	int _minQueueSpaceRequiredForPoll; // Minimum space required in the _queue before a poll request will be made
	int _minRecordCountToPoll;         // The polling loop will not query the db unless it intends to pull down at least this many records
	int _maxErrorSleepRetryTimeMS;     // When an error occurs in the connection/setup stage, wait no more than this long before trying again
	int _loopSleepTimeMinMS;           // Minimum amount of time to wait between polls of the db when the last poll returned no rows
	int _loopSleepTimeMaxMS;           // Maximum amount of time to wait between polls of the db when the last poll returned no rows

	int _minErrorSleepRetryTimeMS;     // When an error occurs in the connection/setup stage, wait at least this long before trying again
	int _maxQueuedRecords;             // Maximum number of records to hold in the _queue
	String _JDBCConnectionString = null;
	String _JDBCDriverClass = null;
	String _JDBCPollForItemsQuery = null;
	String _JDBCNextOffsetQuery = null;
	String _LastOffsetWrittenSaveFile = null;

	String _KafkaTopic = null;

	JDBCHelpers _h;
	ProducerConfig _ProducerConfigSettings;
	Producer<String, String> _ProducerConnection;

	public JDBCReaderThread(Properties props) {
		mapConfigSettings(props);
		_h = new JDBCHelpers(props);

		if(_JDBCDriverClass != null) {
			try {
				// this will load the MySQL driver, each DB has its own driver
				Class.forName (_JDBCDriverClass);
			} catch (java.lang.ClassNotFoundException  cnfEx) {
				LOG.error ("Couldn't load '" + _JDBCDriverClass + "' driver: " + cnfEx.toString());
				return;
			}
		}
		_ProducerConnection = null;
		_ProducerConfigSettings = null;

		LOG.info("Configuring Kafka ...");
		_ProducerConfigSettings = new ProducerConfig(props);

		LOG.info("Attaching to Kafka broker ...");
		_ProducerConnection = new Producer<String, String>(_ProducerConfigSettings);
	}

	private void mapConfigSettings(Properties props) {
		_maxQueuedRecords = this.getInt(props.getProperty("MaxQueuedRecords", "500000"));
		_minQueueSpaceRequiredForPoll = this.getInt(props.getProperty("MinQueueSpaceRequiredForPoll", "250000"));
		_minRecordCountToPoll = this.getInt(props.getProperty("MinRecordCountToPoll", new Integer((_maxQueuedRecords < 1000 ? 1 : (_maxQueuedRecords/10))).toString()));
		_minErrorSleepRetryTimeMS = this.getInt(props.getProperty("MinErrorSleepRetryTimeMS", "500"));
		_maxErrorSleepRetryTimeMS = this.getInt(props.getProperty("MaxErrorSleepRetryTimeMS", "60000"));
		_loopSleepTimeMinMS = this.getInt(props.getProperty("LoopSleepTimeMinMS", "300000"));
		_loopSleepTimeMaxMS = this.getInt(props.getProperty("LoopSleepTimeMaxMS", "900000"));

		_JDBCConnectionString =  props.getProperty("ConnectionString");
		_JDBCDriverClass =  props.getProperty("DriverClass");
		_JDBCPollForItemsQuery = props.getProperty("JDBCPollForItemsQuery", "SELECT * FROM callrecord_copy WHERE id > ? ORDER BY id ASC LIMIT ?");
		_JDBCNextOffsetQuery = props.getProperty("JDBCNextIdQuery", "SELECT id FROM callrecord_copy ORDER BY id ASC LIMIT 1");

		_LastOffsetWrittenSaveFile = props.getProperty("LastOffsetWrittenSaveFile", "LastOffsetWritten.save");

		// @TODO Throw an error if not set
		_KafkaTopic = props.getProperty("KafkaTopic", "newTopic");
	}

	private static Integer getInt(Object o) {
		if(o == null) {
			throw new IllegalArgumentException("NULL object can not be converted to an Integer");
		}

		if(o instanceof Long) {
			return ((Long) o ).intValue();
		} else if (o instanceof Integer) {
			return (Integer) o;
		} else if (o instanceof Short) {
			return ((Short) o).intValue();
		} else if (o instanceof String) {
			return Integer.valueOf((String) o);
		} else {
			throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
		}
	}

	public void open() {
	}

	public void deactivate() {
		synchronized(this ) {
			this._keepGoing = false;
		}
	}

	public void activate() {
		synchronized(this ) {

			this._keepGoing = true;
		}
		this.start();
	}

	public void close() {
		synchronized(this ) {
			this._keepGoing = false;
		}
	}

	// This method determines how long to wait between polling requests when errors have occurred
	private int getErrorSleepDelay(int tryCount ) {
		int rVal = _minErrorSleepRetryTimeMS;
		while(tryCount > 0 ) {
			rVal *= 2;
			--tryCount;
		}
		if(rVal > _maxErrorSleepRetryTimeMS) {
			rVal = _maxErrorSleepRetryTimeMS;
		}
		return(rVal);
	}

	private int getIdleSleepDelay(int tryCount ) {
		int rVal = _loopSleepTimeMinMS;
		while(tryCount > 0 ) {
			rVal *= 2;
			--tryCount;
		}
		if(rVal > _loopSleepTimeMaxMS) {
			rVal = _loopSleepTimeMaxMS;
		}
		return(rVal);
	}

	// This method is used when we want to wait, but we also want the thread to be able to end without
	// hanging around until the timeout occurs.
	private void loopExitableWait(int delayInMilliseconds ) {
		try {
			Thread.sleep(delayInMilliseconds);
		} catch(java.lang.InterruptedException dontCare ) {
		}
	}

	long idLast = 0L;       // this should be populated by getting the last record in the system that was processed
	public void run() {
		Connection dbConnection = null;              // Holds our connection to the db
		PreparedStatement dbReadItemsCmd = null;     // Holds our prepared statement for the query
		boolean restartNeeded = true;                   // Used to flag if we need to (re)connect to the db, this flag is set when pretty much ANY error occurs, causing the connection to the db to be disconnected if open, cleaned up, and reconnected from scratch
		int dbRetries = 0;                           // The number of times this thread has tried to connect to the db and failed.  It is only reset to 0 after a query returns successfully and the expected columns can be mapped to indexes.
		int dbIdles = 0;
		idLast = 0;

		Path file = Paths.get(_LastOffsetWrittenSaveFile);

		try {
			// this try block is to ensure SocketIOServer gets cleaned up (finally)

			LOG.debug ("Starting JDBCReaderThread ...");
			while ( this._keepGoing ) {
				if ( restartNeeded ) {
					++dbRetries;
					if(dbConnection != null ) {
						if(dbReadItemsCmd != null ) {
							try {
								if(!dbReadItemsCmd.isClosed() ) {
									dbReadItemsCmd.close();
								}
							} catch(SQLException sqlEx ) {
								LOG.error ("Could not close prepared statement for connection restart (try: " + dbRetries + "): " + sqlEx.toString());
								restartNeeded = true;
								this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
								continue;
							} finally {
								dbReadItemsCmd = null;
							}
						}
						if(dbConnection != null ) {
							try {
								if(!dbConnection.isClosed() ) {
									dbConnection.close();
								}
							} catch(SQLException sqlEx ) {
								LOG.error ("Could not close db connection for connection restart (try: " + dbRetries + "): " + sqlEx.toString());
								restartNeeded = true;
								this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
								continue;
							} finally {
								dbConnection = null;
							}
						}
						dbConnection = null;
					}
					try {
						// setup the connection with the DB.
						dbConnection = DriverManager.getConnection(this._JDBCConnectionString);
					} catch(SQLException sqlEx ) {
						LOG.error ("Could not open db connection: " + this._JDBCConnectionString + ": " + sqlEx.toString());
						dbConnection = null;
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					}
					try {
						// preparedStatements can use variables and are more efficient
						dbReadItemsCmd = dbConnection.prepareStatement (_JDBCPollForItemsQuery);
					} catch(SQLException sqlEx ) {
						LOG.error ("Could not complete prepared statement setup for db polling (try: " + dbRetries + "): " + sqlEx.toString());
						try {
							dbConnection.close();
							dbConnection = null;
						} catch(SQLException sqlExx ) {
							LOG.error ("Could not close db connection when cleaning up failed prepared statement setup (try: " + dbRetries + "): " + sqlExx.toString());
						}
						dbReadItemsCmd = null;
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					}
					// look for a file first, if not go to the DB for our next ID
					idLast = 0;
					try {
						if(Files.exists(file)) {
							// Looks like we have a save file from a previous run, lets see if we can parse it
							List<String> lines = Files.readAllLines(file);
							idLast = Long.parseLong(lines.get(0));
						}
					} catch (IOException e) {
						e.printStackTrace();
						idLast = 0;
					}

					if(idLast == 0) {
						// Couldn't load last ID from file, try from the DB instead
						Statement getNextId = null;
						try {
							getNextId = dbConnection.createStatement();
							ResultSet nrs = getNextId.executeQuery(_JDBCNextOffsetQuery);
							idLast = 0;
							if (nrs.next()) {
								idLast = nrs.getLong(1);
								LOG.info("Initial LastID value is: " + idLast);
							} else {
								LOG.error("Initial LastID value lookup returned no results!");
							}
							nrs.close();
							if (idLast == 0) {
								LOG.error("Initial LastID value is: 0");
								restartNeeded = true;
								this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
								continue;
							}
						} catch (SQLException sqlEx) {
							LOG.error("Could not complete prepared statement setup for db polling (try: " + dbRetries + "): " + sqlEx.toString());
							try {
								dbConnection.close();
							} catch (SQLException sqlExx) {
								LOG.error("Could not close db connection when cleaning up failed prepared statement setup: " + sqlExx.toString());
							}
							dbConnection = null;
							dbReadItemsCmd = null;
							restartNeeded = true;
							this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
							continue;
						} finally {
							try {
								getNextId.close();
							} catch (SQLException sqlExx) {
								LOG.error("Could not close statement for initial db index: " + sqlExx.toString());
							}
						}
					}
					restartNeeded = false;
				}

				DecimalFormat numberFormat = new DecimalFormat("####.#######");
				try {
					dbReadItemsCmd.setLong(1, idLast);
					dbReadItemsCmd.setLong(2, _maxQueuedRecords);
					LOG.debug ("Performing database polling query, attempting to fetch " + _maxQueuedRecords + " rows ...");
					ResultSet resultSet;
					try {
						resultSet = dbReadItemsCmd.executeQuery();
					} catch(SQLException sqlEx ) {
						LOG.error("Could not query db: executeQuery failed: " + sqlEx.toString());
						LOG.error("Could not query db: Query was: " + _JDBCPollForItemsQuery);
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					}

					if(resultSet != null ) {
						LOG.debug("Polling query completed, mapping column index values ...");
						ResultSetMetaData rsmd = resultSet.getMetaData();
						int numColumns = rsmd.getColumnCount();
						int item_id = getResultSetFieldIndex ("id", resultSet);

						LOG.debug ("Mapping column index values complete.");
						long unique_id = idLast;
						int resultCount = 0;
						long processingTime = -1;
						long startTime = System.currentTimeMillis();
						boolean outputKafka = false;

						try {
							if( resultSet.next()) {
								while (_keepGoing) {
									unique_id = resultSet.getLong(item_id);
									JSONObject obj = new JSONObject();


									for (int i = 1; i < numColumns + 1; i++) {
										String column_name = rsmd.getColumnName(i);

										if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
											obj.put(column_name, resultSet.getArray(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
											obj.put(column_name, resultSet.getLong(column_name));
										} else if (rsmd.getColumnType(i) == Types.INTEGER) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
											obj.put(column_name, resultSet.getBoolean(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
											obj.put(column_name, resultSet.getBlob(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
											obj.put(column_name, resultSet.getDouble(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
											obj.put(column_name, resultSet.getFloat(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
											obj.put(column_name, resultSet.getNString(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
											obj.put(column_name, resultSet.getString(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
											obj.put(column_name, resultSet.getDate(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.TIME) {
											obj.put(column_name, resultSet.getTime(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
											obj.put(column_name, resultSet.getTimestamp(column_name));
										} else {
											obj.put(column_name, resultSet.getObject(column_name));
										}
									}

									try {
										String encodedKafkaMessage = obj.toString();
										if(_ProducerConnection != null) { // if _ProducerConnection is NULL, it means we don't want to write to Kafka
											KeyedMessage<String, String> data = new KeyedMessage<String, String>(_KafkaTopic, null, encodedKafkaMessage);
											_ProducerConnection.send(data);
										}

										idLast = unique_id; // WE only update idLast if we successfully sent the message
										try {
											Files.write(file, Arrays.asList(String.format("%d", idLast)), Charset.forName("UTF-8"), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
										} catch (IOException e) {
											e.printStackTrace();
										}
										++resultCount;
										if (resultSet.next() == false) {
											// No more results to process, bail out
											break;
										}
									} catch (Exception ex) {
										int kafkaFailureSleepTimeInMS = 500;
										LOG.error("Queue attempt failed: record id: " + unique_id + ": " + ex.getClass() + ": " + ex.getMessage());
										LOG.warn("Retry throttled, sleeping for " + kafkaFailureSleepTimeInMS + "ms");
										try {
											Thread.sleep(kafkaFailureSleepTimeInMS);
										} catch (InterruptedException e) {
											LOG.error(e);
										}
									}
								}
							}
							processingTime = System.currentTimeMillis() - startTime;
							// If we get to here, we consider the connection 'successful, and zero out 'retries'
							dbRetries = 0;
						} finally {
							resultSet.close();
						}
						if(!_keepGoing ) {
							// this will cause us to break out of the loop
							break;
						}
						// Yield processing for a brief moment, this essentially causes nothing more than a yield() to other threads as a curtesy
						int loopSleepTime = 1;
						if(resultCount > _minRecordCountToPoll ) {
							dbIdles = 0;
							LOG.info ("Queued " + resultCount + " records in " + (processingTime / 1000.0) + "s (" + Math.round(resultCount / (processingTime/1000.0)) +  "/sec), last ID was: " + unique_id);
							continue;
						} else if(resultCount > 0 ) {
							// Less than 10k records, take a break
							loopSleepTime = getIdleSleepDelay(++dbIdles);
							LOG.info ("Queued " + resultCount + " records in " + (processingTime / 1000.0) + "s (" + Math.round(resultCount / (processingTime/1000.0)) +  "/sec), last ID was: " + unique_id + ", sleeping for " + loopSleepTime/1000.0 + "s");
							loopSleepTime = 500;
						} else {
							loopSleepTime = getIdleSleepDelay(++dbIdles);
							LOG.info ("Too few results returned (" + resultCount + ") idling for " + (loopSleepTime / 1000.0 ) + " seconds");
							loopSleepTime = 500;
						}
						this.loopExitableWait(loopSleepTime);
					} else {
						LOG.error ("No result set returned?!?!");
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
					}
				} catch(SQLException sqlEx ) {
					LOG.error ("Could not close db connection when processing query results: " + sqlEx.toString());
					restartNeeded = true;
					this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
					continue;
				}
			}
		} finally {
			try {
				if(dbConnection != null ) {
					if(!dbConnection.isClosed() ) {
						dbConnection.close();
					}
				}
			} catch(SQLException sqlEx ) {
				LOG.error("Could not close db connection when deactivating spout: " + sqlEx.toString());
			}
		}
	}

	private int getResultSetFieldIndex(String desiredColumnName, ResultSet resultSet ) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		for(int i = 1; i <= columnCount; i++ ) {
			if(metaData.getColumnName(i).equals(desiredColumnName)) {
				return(i);
			}
		}
		throw new SQLException ("Column name requested could not be found in ResultSet: " + desiredColumnName);
	}
}
