import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

	static long st;
	
  public static void main(String[] args) {
	long st = System.currentTimeMillis();
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = null;
    String sql = "";
	JDBCConnectionManager jdbcConnectionManager = new JDBCConnectionManager();
	PreparedStatement preparedStatement = null;
	ResultSet resultSet = null;
	Connection connection = null;
	jdbcConnectionManager.getJDBCConnection();

	try {
      producer = new KafkaProducer<>(props);
      
      connection = jdbcConnectionManager.getConnection();
      sql = "SELECT COL1 FROM SOURCE_TAB";
      preparedStatement = connection.prepareStatement(sql);
      resultSet = preparedStatement.executeQuery();
	
      while (resultSet.next()) {
    	  producer.send(new ProducerRecord<String, String>("HelloKafkaTopic", String.valueOf(resultSet.getInt(1))));
      }
      
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      producer.close();
      jdbcConnectionManager.closeConnection(connection, preparedStatement, resultSet);
    }
  }
}
