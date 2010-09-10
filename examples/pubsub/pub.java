
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class pub {
    public static void main(String[] args) {
        try {

            //  By default connect to the local AMQP broker
            String hostName = (args.length > 0) ? args[0] : "localhost";
            int portNumber = (args.length > 1) ?
                Integer.parseInt(args[1]) : AMQP.PROTOCOL.PORT;

            //  Connect to the AMQP broker
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(hostName);
            factory.setPort(portNumber);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            for (;;) {

                //  Send a message.
                channel.basicPublish("PUBSUB", null, null,
                    "Hello, World???".getBytes());

                //  Sleep for one second.
                Thread.sleep (1000);
            }
        } catch (Exception e) {
            System.err.println("Main thread caught exception: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
