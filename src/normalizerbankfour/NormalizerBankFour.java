/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package normalizerbankfour;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import dk.cphbusiness.connection.ConnectionCreator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import utilities.xml.xmlMapper;

/**
 *
 * @author Kasper
 */
public class NormalizerBankFour {

    private static final String IN_QUEUE = "bank_four_normalizer_gr1";
    private static final String OUT_QUEUE = "aggregator_gr1";
    private static Channel channelIn;
    private static Channel channelOut;
    private static QueueingConsumer consumer;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ConnectionCreator creator = ConnectionCreator.getInstance();
        try {
            channelIn = creator.createChannel();
            channelIn.queueDeclare(IN_QUEUE, false, false, false, null);
            channelOut = creator.createChannel();
            channelOut.queueDeclare(OUT_QUEUE, false, false, false, null);
            consumer = new QueueingConsumer(channelIn);
            channelIn.basicConsume(IN_QUEUE, consumer);

        } catch (IOException ex) {
            Logger.getLogger(NormalizerBankFour.class.getName()).log(Level.SEVERE, null, ex);
        }

        while (true) {
            try {
                System.out.println("Normalizer for BankFour is running");
                Delivery delivery = consumer.nextDelivery();
                channelIn.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println("Got message: " + new String(delivery.getBody()));
                String message = normalizeMessage(new String(delivery.getBody()));
                BasicProperties probs = new BasicProperties().builder().correlationId(delivery.getProperties().getCorrelationId()).build();
                channelOut.basicPublish("", OUT_QUEUE, probs, message.getBytes());
            } catch (InterruptedException ex) {
                Logger.getLogger(NormalizerBankFour.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ShutdownSignalException ex) {
                Logger.getLogger(NormalizerBankFour.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ConsumerCancelledException ex) {
                Logger.getLogger(NormalizerBankFour.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(NormalizerBankFour.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private static String normalizeMessage(String message) {
        XPath xPath = XPathFactory.newInstance().newXPath();
        Document doc = xmlMapper.getXMLDocument(message);
        try {
            String ssn = xPath.compile("/LoanResponse/ssn").evaluate(doc);
            ssn = ssn.substring(0,6) + "-" + ssn.substring(6);
            doc.getElementsByTagName("ssn").item(0).getFirstChild().setNodeValue(ssn);
        } catch (XPathExpressionException ex) {
            Logger.getLogger(NormalizerBankFour.class.getName()).log(Level.SEVERE, null, ex);
        }
        String result = xmlMapper.getStringFromDoc(doc);
        System.out.println("reply: " + result);
        return result;
    }
}
