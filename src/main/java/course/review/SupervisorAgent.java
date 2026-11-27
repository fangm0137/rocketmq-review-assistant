package course.review;

import com.fasterxml.jackson.databind.ObjectMapper;
import course.review.ReviewAgents.Agent;
import course.review.ReviewAgents.Result;
import course.review.ReviewAgents.Review;
import course.review.ReviewAgents.Task;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.a2a.common.model.RocketMQRequest;
import org.apache.rocketmq.a2a.common.model.RocketMQResponse;
import org.apache.rocketmq.a2a.common.util.RocketMQUtil;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;

public final class SupervisorAgent {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();
    private static final String STUDENT = "2601210305";
    private static final String TASK_TOPIC = "rmqai-" + STUDENT + "-review-task";
    private static final String LITE_PARENT = "rmqai-" + STUDENT + "-review-response";
    private static final String RESPONSE_GROUP = group("supervisor");
    private static final int INPUT_LIMIT = 32 * 1024;
    private static final int WAIT_SECONDS = 60;
    private static final List<Agent> REVIEWERS = List.of(new ReviewAgents.StyleAgent(),
            new ReviewAgents.LogicAgent(), new ReviewAgents.SecurityAgent());
    private static final Agent SUMMARY = new ReviewAgents.SummaryAgent();
    private final String session = STUDENT + "-" + UUID.randomUUID();
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final ClientConfiguration configuration;
    private final Map<String, CompletableFuture<Result>> replies = new LinkedHashMap<>();

    private SupervisorAgent(String endpoint) {
        this.endpoint = endpoint;
        boolean local = endpoint.startsWith("127.0.0.1:") || endpoint.startsWith("localhost:");
        accessKey = credential("REVIEW_MQ_ACCESS_KEY", local);
        secretKey = credential("REVIEW_MQ_SECRET_KEY", local);
        configuration = ClientConfiguration.newBuilder().setEndpoints(endpoint)
                .setCredentialProvider(new StaticSessionCredentialsProvider(accessKey, secretKey)).build();
        for (Agent agent : REVIEWERS) {
            replies.put(agent.role(), new CompletableFuture<>());
        }
        replies.put(SUMMARY.role(), new CompletableFuture<>());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Usage: SupervisorAgent endpoint source.java output-directory");
        }
        Path source = Path.of(args[1]);
        String sourceName = source.getFileName().toString();
        if (!sourceName.endsWith(".java") || Files.size(source) > INPUT_LIMIT) {
            throw new IllegalArgumentException("Select a Java source no larger than 32 KiB.");
        }
        String code = Files.readString(source, StandardCharsets.UTF_8);
        if (code.isBlank() || code.getBytes(StandardCharsets.UTF_8).length > INPUT_LIMIT) {
            throw new IllegalArgumentException("Expected a nonempty Java source no larger than 32 KiB.");
        }
        Path destination = Files.createDirectories(Path.of(args[2]));
        new SupervisorAgent(args[0]).review(sourceName, code, destination);
    }

    private void review(String sourceName, String source, Path destination) throws Exception {
        RocketMQUtil.checkConfigParam(endpoint, LITE_PARENT, RESPONSE_GROUP, session, TASK_TOPIC);
        log("SESSION", "SUPERVISOR", LITE_PARENT, RESPONSE_GROUP, session, "-");
        try (Producer producer = RocketMQUtil.buildProducer("", endpoint, accessKey, secretKey,
                     TASK_TOPIC, LITE_PARENT);
             var receiver = RocketMQUtil.buildLitePushConsumer(endpoint, "", accessKey,
                     secretKey, LITE_PARENT, RESPONSE_GROUP, this::onResponse)) {
            receiver.subscribeLite(session);
            List<PushConsumer> workers = new ArrayList<>();
            try {
                for (Agent worker : REVIEWERS) {
                    workers.add(startWorker(producer, worker));
                }
                workers.add(startWorker(producer, SUMMARY));
                List<CompletableFuture<SendReceipt>> sends = new ArrayList<>();
                for (Agent worker : REVIEWERS) {
                    sends.add(dispatch(producer, new Task(session, worker.role(), sourceName, source, List.of())));
                }
                awaitAll(sends);
                awaitAll(REVIEWERS.stream().map(worker -> replies.get(worker.role())).toList());
                List<Review> reviews = REVIEWERS.stream()
                        .map(worker -> replies.get(worker.role()).join().review()).toList();
                dispatch(producer, new Task(session, SUMMARY.role(), sourceName, "", reviews))
                        .get(WAIT_SECONDS, TimeUnit.SECONDS);
                Result summary = replies.get(SUMMARY.role()).get(WAIT_SECONDS, TimeUnit.SECONDS);
                Files.writeString(destination.resolve("review.md"), summary.report(), StandardCharsets.UTF_8);
                JSON.writerWithDefaultPrettyPrinter().writeValue(destination.resolve("reviews.json").toFile(), reviews);
            } finally {
                closeWorkers(workers);
            }
        }
        System.out.println("REVIEW_PASS session=" + session + " workers=3 summary=1");
    }

    private PushConsumer startWorker(Producer producer, Agent agent) throws Exception {
        String group = group(agent.role());
        return PROVIDER.newPushConsumerBuilder().setClientConfiguration(configuration)
                .setConsumerGroup(group)
                .setSubscriptionExpressions(Map.of(TASK_TOPIC,
                        new FilterExpression(agent.role(), FilterExpressionType.TAG)))
                .setMessageListener(message -> onTask(producer, agent, group, message))
                .build();
    }

    private CompletableFuture<SendReceipt> dispatch(Producer producer, Task task) throws IOException {
        String key = taskKey(task.agent());
        RocketMQRequest request = RocketMQRequest.builder().requestId(key)
                .destAgentTopic(TASK_TOPIC).workAgentResponseTopic(LITE_PARENT).liteTopic(session)
                .requestBody(JSON.writeValueAsString(task)).build();
        Message message = PROVIDER.newMessageBuilder().setTopic(TASK_TOPIC).setTag(task.agent())
                .setKeys(key).setBody(JSON.writeValueAsBytes(request)).build();
        return producer.sendAsync(message).thenApply(receipt -> {
            log("TASK_SEND", task.agent(), TASK_TOPIC, group(task.agent()), key, receipt.getMessageId().toString());
            return receipt;
        });
    }

    private ConsumeResult onTask(Producer producer, Agent agent, String group, MessageView message) {
        try {
            RocketMQRequest request = JSON.readValue(body(message), RocketMQRequest.class);
            String key = taskKey(agent.role());
            if (!session.equals(request.getLiteTopic()) || !key.equals(request.getRequestId())
                    || !message.getKeys().contains(key)
                    || !TASK_TOPIC.equals(request.getDestAgentTopic())
                    || !LITE_PARENT.equals(request.getWorkAgentResponseTopic())) {
                return ConsumeResult.SUCCESS;
            }
            Task task = JSON.readValue(request.getRequestBody(), Task.class);
            if (!session.equals(task.session()) || !agent.role().equals(task.agent())) {
                return ConsumeResult.SUCCESS;
            }
            log("TASK_RECEIVE", agent.role(), TASK_TOPIC, group, key, message.getMessageId().toString());
            Result result = agent.execute(task);
            RocketMQResponse envelope = RocketMQResponse.builder().requestId(key)
                    .contextId(session).taskId(key).end(true)
                    .responseBody(JSON.writeValueAsString(result)).build();
            String responseKey = resultKey(agent.role());
            Message response = PROVIDER.newMessageBuilder().setTopic(LITE_PARENT)
                    .setLiteTopic(session).setKeys(responseKey)
                    .setBody(JSON.writeValueAsBytes(envelope)).build();
            SendReceipt receipt = producer.send(response);
            log("LITE_SEND", agent.role(), LITE_PARENT, group, responseKey, receipt.getMessageId().toString());
            log("TASK_COMPLETE", agent.role(), TASK_TOPIC, group, key, message.getMessageId().toString());
            return ConsumeResult.SUCCESS;
        } catch (Exception e) {
            System.err.println("Worker " + agent.role() + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return ConsumeResult.FAILURE;
        }
    }

    private ConsumeResult onResponse(MessageView message) {
        try {
            RocketMQResponse envelope = JSON.readValue(body(message), RocketMQResponse.class);
            if (!session.equals(envelope.getContextId())) {
                return ConsumeResult.SUCCESS;
            }
            Result result = JSON.readValue(envelope.getResponseBody(), Result.class);
            CompletableFuture<Result> reply = replies.get(result.agent());
            if (reply != null && session.equals(result.session())
                    && taskKey(result.agent()).equals(envelope.getRequestId())
                    && message.getKeys().contains(resultKey(result.agent()))) {
                log("LITE_RECEIVE", result.agent(), LITE_PARENT, RESPONSE_GROUP,
                        resultKey(result.agent()), message.getMessageId().toString());
                reply.complete(result);
            }
            return ConsumeResult.SUCCESS;
        } catch (Exception e) {
            System.err.println("Supervisor: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return ConsumeResult.FAILURE;
        }
    }

    private static byte[] body(MessageView message) {
        var data = message.getBody().asReadOnlyBuffer();
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return bytes;
    }

    private static void awaitAll(List<? extends CompletableFuture<?>> futures) throws Exception {
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static void closeWorkers(List<PushConsumer> workers) {
        for (PushConsumer worker : workers) {
            try {
                worker.close();
            } catch (IOException e) {
                System.err.println("Unable to close worker: " + e.getMessage());
            }
        }
    }

    private static String credential(String name, boolean local) {
        String value = System.getenv(name);
        if (value != null && !value.isBlank()) {
            return value;
        }
        if (local) {
            return "local-review";
        }
        throw new IllegalArgumentException("Set " + name + " for a remote RocketMQ endpoint.");
    }

    private static String group(String role) {
        return "GID_" + STUDENT + "_" + role.toLowerCase(Locale.ROOT);
    }

    private String taskKey(String role) {
        return session + "-task-" + role.toLowerCase(Locale.ROOT);
    }

    private String resultKey(String role) {
        return session + "-result-" + role.toLowerCase(Locale.ROOT);
    }

    private void log(String event, String role, String topic, String group, String key, String id) {
        System.out.printf("TRACE\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%n",
                Instant.now(), session, event, role, topic, group, key, id);
    }
}
