package us.wimsey.jdbc2kafka;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.util.Enumeration;
import java.util.Properties;

public class JDBC2Kafka {
    private static final Logger LOG = Logger.getLogger ( JDBC2Kafka.class );

    public static void main(String[] args) throws Exception {
    	BasicConfigurator.configure(); // Basic console output for log4j
    	LogManager.getRootLogger().setLevel(Level.WARN);

		Options options = new Options();
		options.addOption("help", false, "Display help");
		options.addOption("properties", true, "Properties file to load for configuration properties");


		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse( options, args );

		if(line.hasOption("help") == true) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "jdbc2kafka", options );
			return;
		}

		String propertiesFile = "JDBC2Kafka.properties";
		if( line.hasOption( "properties" ) == true) {
			if (line.getOptionValue("properties") != null) {
				propertiesFile = line.getOptionValue("properties");
			}
		}

		int nextArg = 0;
 		String defaultPropertiesFile = "JDBC2KafkaDefaults.properties";

		Properties defaults = loadPropertiesResourceFile(null, defaultPropertiesFile);
		Properties props = loadPropertiesFile(defaults, propertiesFile);
		
		if(props.containsKey("metadata.broker.list")) {
			LOG.info("OVERRIDE: metadata.broker.list=" + props.get("metadata.broker.list"));
		} else {
			props.put("metadata.broker.list", "localhost:9092");
		}

		if(props.containsKey("serializer.class")) {
			LOG.info("OVERRIDE: serializer.class=" + props.get("serializer.class"));
		} else {
			props.put("serializer.class", "kafka.serializer.StringEncoder");
		}

		if(props.containsKey("partitioner.class")) {
			LOG.info("OVERRIDE: partitioner.class=" + props.get("partitioner.class"));
		} else {
			props.put("partitioner.class", "us.wimsey.jdbc2kafka.RoundRobinPartitioner");
		}

		if(props.containsKey("request.required.acks")) {
			LOG.info("OVERRIDE: request.required.acks=" + props.get("request.required.acks"));
		} else {
			props.put("request.required.acks", "1");
		}


		String driverClassName = props.getProperty("DriverClass");
		if(driverClassName != null) {
			try {
				Class.forName(driverClassName);
			} catch (java.lang.ClassNotFoundException cnfEx) {
				LOG.error("Couldn't load '" + driverClassName + "' driver: " + cnfEx.toString());
				return;
			}
		}

		//KafkaProducer rtw = new KafkaProducer(props);
        JDBCReaderThread reader_thread = new JDBCReaderThread(props);

 //       LOG.info("Configuring Kafka ...");
 //   	ProducerConfig _ProducerConfigSettings = new ProducerConfig(props);

//		LOG.info("Attaching to Kafka broker ...");
//	    Producer<String, String> _ProducerConnection = new Producer<String, String>(_ProducerConfigSettings);


        reader_thread.activate();
        reader_thread.join();
/*		try {
			Map tuple;
			int idleCount = 0;
			while(true) {
			
				if ( reader_thread._queue.size() > 0 ) {
					tuple = reader_thread._queue.take();

					KeyedMessage<String, String> data = new KeyedMessage<String, String>("kafkaTopic", null, tuple.toString());
					_ProducerConnection.send(data);
					idleCount = 0;
				} else {
				LOG.warn(".");
					if ( idleCount % 11 == 10 ) {
						LOG.info ( "Idle spout: No tuples in queue." );
					}
					++idleCount;
					Thread.sleep (500);
				}
			
				Thread.sleep(1);
			}
		} finally {
			_ProducerConnection.close();
		}
  */
  	}

	public static Properties loadPropertiesResourceFile(Properties existingProperties, String propertiesFilename) {
		InputStream input = null;

		try {
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			input = classloader.getResourceAsStream(propertiesFilename);

			LOG.debug("Property resource file read: " + propertiesFilename);
			return(loadPropertiesFile(existingProperties, input));
		} catch (FileNotFoundException ex) {
			System.out.println("Properties resource file could not be found: " + propertiesFilename);
			return(new Properties());	// throw out the properties file data if we couldn't load it for some reason, it may be corrupted if we only got a partial load
		} catch (IOException ex) {
			return(new Properties());	// throw out the properties file data if we couldn't load it for some reason, it may be corrupted if we only got a partial load
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static Properties loadPropertiesFile(Properties existingProperties, String propertiesFilename) {
		InputStream input = null;

		try {
			input = new FileInputStream(propertiesFilename);
			LOG.debug("Property file read: " + propertiesFilename);
			return(loadPropertiesFile(existingProperties, input));
		} catch (FileNotFoundException ex) {
			System.out.println("Properties could not be found: " + propertiesFilename);
			return(new Properties());	// throw out the properties file data if we couldn't load it for some reason, it may be corrupted if we only got a partial load
		} catch (IOException ex) {
			return(new Properties());	// throw out the properties file data if we couldn't load it for some reason, it may be corrupted if we only got a partial load
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static Properties loadPropertiesFile(Properties existingProperties, InputStream input) throws IOException {
		Properties prop;
		if(existingProperties == null) {
			prop = new Properties();
		} else {
			prop = new Properties(existingProperties);
		}

		// load a properties file
		prop.load(input);

		int propertiesCount = 0;
		Enumeration<?> e = prop.propertyNames();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			String value = prop.getProperty(key);
			LOG.info("conf.put(\"" + key + "\", \"" + value + "\");");                // Maximum number of records to hold in the queue
			//conf.put(key, value);
			++propertiesCount;
		}
		if(propertiesCount > 1) {
			LOG.info("Property file loaded: " + propertiesCount + " properties processed.");
		} else if(propertiesCount == 1) {
			LOG.info("Property file loaded: 1 property processed.");
		} else {
			LOG.warn("Property file loaded: no properties processed.");
		}

		return(prop);
	}
}
