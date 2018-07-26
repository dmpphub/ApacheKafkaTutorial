import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerTest {

  public static void main(String[] args) throws Exception {
	long st = System.currentTimeMillis();
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group-1");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    String sql = "";
	JDBCConnectionManager jdbcConnectionManager = new JDBCConnectionManager();
	PreparedStatement preparedStatement = null;
	ResultSet resultSet = null;
	Connection connection = null;
	
	jdbcConnectionManager.getJDBCConnection();
	connection = jdbcConnectionManager.getConnection();
    
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    kafkaConsumer.subscribe(Arrays.asList("HelloKafkaTopic"));
    sql = "INSERT INTO TARGET_TAB VALUES (?)";
	preparedStatement = connection.prepareStatement(sql);
    
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
      
      for (ConsumerRecord<String, String> record : records) {
        /*System.out.println("Partition: " + record.partition() + " Offset: " + record.offset()
            + " Value: " + record.value() + " ThreadID: " + Thread.currentThread().getId());*/
    	  preparedStatement.setInt(1, Integer.parseInt(record.value()));
    	  preparedStatement.executeUpdate();  
    	  
      }
      long end = System.currentTimeMillis();
      System.out.println("End Time : " + TimeUnit.MILLISECONDS.toSeconds(end - st) + " Seconds");
    }
    
  }

}