import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SQSExample {

    public static void main(String[] args) {

        SqsClient sqsClient = SqsClient.builder()
                .region(Region.EU_CENTRAL_1)
                .build();
        String url = "https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK";

//        createQueue(sqsClient,"testQueueWithJavaSDK");
//        listQueues(sqsClient);

//        sendMessage(sqsClient,"https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK","message1");
//        sendMessage(sqsClient,"https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK","message2");
//        sendMessage(sqsClient,"https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK","message3");
//        sendMessage(sqsClient,"https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK","message4");
//        sendMessage(sqsClient,"https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK","message5");
//        sendBatchMessages(sqsClient,"https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK");
//        String message ="{" +
//                "  \"count\": 12," +
//                "  \"offset\": 34" +
//                "}";
//
//        sendMessage(sqsClient,url,message);

//        for (int i = 0; i < 100; i++) {
//            sendMessage(sqsClient,url, UUID.randomUUID().toString());
//        }


        List<Message> messageList = receiveMessagesTest(sqsClient, "https://sqs.eu-central-1.amazonaws.com/438375164028/testQueueWithJavaSDK", 10);

        for (Message m : messageList) {
            System.out.println(m.body());
        }


    }

    public static void createQueue(SqsClient sqsClient, String queueName) {

        try {
            System.out.println("\nCreate Queue");

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            System.out.println("\nGet queue url");

//            // snippet-start:[sqs.java2.sqs_example.get_queue]
//            GetQueueUrlResponse getQueueUrlResponse =
//                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
//            String queueUrl = getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        // snippet-end:[sqs.java2.sqs_example.get_queue]
    }

    public static void listQueues(SqsClient sqsClient) {

        System.out.println("\nList Queues");
        String prefix = "que";

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder()
                    //.queueNamePrefix(prefix) //префикс для имени очереди, опционально
                    .build();
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

            for (String url : listQueuesResponse.queueUrls()) {
                System.out.println(url);
            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void sendMessage(SqsClient sqsClient, String queueUrl, String messageBody) {
        try {
            // snippet-start:[sqs.java2.sqs_example.send_message]
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .delaySeconds(10)
                    .build());
            // snippet-end:[sqs.java2.sqs_example.send_message]
            Message message = Message.builder()
                    .body("{" +
                            "  \"count\": 0," +
                            "  \"offset\": 0" +
                            "}")
                    .build();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void sendBatchMessages(SqsClient sqsClient, String queueUrl) {

        System.out.println("\nSend multiple messages");

        try {
            // snippet-start:[sqs.java2.sqs_example.send__multiple_messages]
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody("btch msg1").build(),
                            SendMessageBatchRequestEntry.builder().id("id2").messageBody("btch msg2").delaySeconds(10).build())
                    .build();
            sqsClient.sendMessageBatch(sendMessageBatchRequest);
            // snippet-end:[sqs.java2.sqs_example.send__multiple_messages]

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl, int maxNumberOfMessages) {

        System.out.println("\nReceive messages");

        try {
            // snippet-start:[sqs.java2.sqs_example.retrieve_messages]
            ReceiveMessageRequest receiveMessageRequest;
            List<Message> messages = new ArrayList<>();
            do {
                receiveMessageRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .waitTimeSeconds(2)
                        .build();
                messages.addAll(sqsClient.receiveMessage(receiveMessageRequest).messages());

                deleteMessages(sqsClient, queueUrl, messages);
            } while (sqsClient.receiveMessage(receiveMessageRequest).hasMessages());

            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
        // snippet-end:[sqs.java2.sqs_example.retrieve_messages]
    }

    public static List<Message> receiveMessagesTest(SqsClient sqsClient, String queueUrl, int maxNumberOfMessages) {
        List<Message> messagelist = new ArrayList<>();

        try {
            boolean flag = true;

            while (flag) {
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(maxNumberOfMessages)
                        .build();
                List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

                //   System.out.println("    Body:          " + message.getBody());
                messagelist.addAll(messages);
                deleteMessages(sqsClient,queueUrl,messages);
                if (messages.size() == 0) {
                    flag = false;
                }
            }

        } catch (SqsException sqs) {
            sqs.printStackTrace();
        } finally {
            return messagelist;
        }
    }

    public static void deleteMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {
        System.out.println("\nDelete Messages");
        // snippet-start:[sqs.java2.sqs_example.delete_message]

        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
//            DeleteMessageBatchRequest  deleteMessageBatchRequest = DeleteMessageBatchRequest
//                    .builder()
//                    .queueUrl(queueUrl)
//                    .entries(messages)
//                    .build();
            // snippet-end:[sqs.java2.sqs_example.delete_message]

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


}
