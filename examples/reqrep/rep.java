
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class rep {
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

            //  Establish the REQ/REP wiring; the service is called HELLO_WORLD
            channel.queueDeclare("HELLO_WORLD", true, false, false, null);
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume("HELLO_WORLD", true, consumer);

            for (;;) {

                //  Get next request
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String replyTo = delivery.getProperties().getReplyTo();

                //  Send the reply
                channel.basicPublish(null, replyTo, null, "World!".getBytes());
            }
        } catch (Exception e) {
            System.err.println("Main thread caught exception: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
