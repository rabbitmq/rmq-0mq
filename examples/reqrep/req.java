
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class req {
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

            //  Establish the REQ/REP wiring.
            String queueName = channel.queueDeclare().getQueue();
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);

            for (;;) {

                //  Send the request
                AMQP.BasicProperties properties = new AMQP.BasicProperties();
                properties.setReplyTo(queueName);
                channel.basicPublish(null, "HELLO_WORLD", properties,
                    "Hello!".getBytes());

                //  Get and print the reply
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String reply = delivery.getBody().toString();
                System.out.println(reply);
            }
        } catch (Exception e) {
            System.err.println("Main thread caught exception: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
