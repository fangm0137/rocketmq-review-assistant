import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDataEncoder;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.topic.TopicValidator;

/**
 * Bridges the actual gRPC receipt and listener events to RocketMQ's native trace format.
 * The lab invokes this only after confirming the indexed messages via Broker queries.
 */
class PublishTrace {
    record Event(Instant time, String name, String role, String topic, String group, String key, String id) {}

    public static void main(String[] args) throws Exception {
        Map<String, Map<String, Event>> messages = new LinkedHashMap<>();
        String log = new String(Files.readAllBytes(Path.of(args[1])), StandardCharsets.UTF_8);
        for (String line : log.lines().toList()) {
            if (!line.startsWith("TRACE\t")) {
                continue;
            }
            String[] part = line.split("\t", -1);
            Event event = new Event(Instant.parse(part[1]), part[3], part[4], part[5], part[6], part[7], part[8]);
            if (!event.name().equals("SESSION")) {
                messages.computeIfAbsent(event.id(), ignored -> new LinkedHashMap<>())
                        .putIfAbsent(event.name(), event);
            }
        }
        if (messages.size() != 8) {
            throw new IllegalStateException("Expected eight observed business messages");
        }
        DefaultMQProducer producer = new DefaultMQProducer("GID_2601210305_review_trace");
        producer.setNamesrvAddr(args[0]);
        producer.setVipChannelEnabled(false);
        producer.start();
        try {
            for (Map<String, Event> stages : messages.values()) {
                boolean lite = stages.containsKey("LITE_SEND");
                Event sent = stages.get(lite ? "LITE_SEND" : "TASK_SEND");
                Event received = stages.get(lite ? "LITE_RECEIVE" : "TASK_RECEIVE");
                Event completed = lite ? received : stages.get("TASK_COMPLETE");
                if (sent == null || received == null || completed == null || !sent.key().equals(received.key())) {
                    throw new IllegalStateException("Cannot publish an incomplete trace");
                }
                TraceBean bean = new TraceBean();
                bean.setMsgId(sent.id());
                bean.setTopic(sent.topic());
                bean.setKeys(sent.key());
                bean.setTags(lite ? "" : sent.role());
                bean.setStoreHost("127.0.0.1:11911");
                bean.setClientHost("127.0.0.1");
                bean.setMsgType(MessageType.Normal_Msg);
                bean.setStoreTime(sent.time().toEpochMilli());
                String wire = encode(TraceType.Pub, sent, lite ? sent.group() : args[2], bean)
                        + encode(TraceType.SubBefore, received, received.group(), bean)
                        + encode(TraceType.SubAfter, completed, received.group(), bean);
                Message trace = new Message(TopicValidator.RMQ_SYS_TRACE_TOPIC, "A2A_REVIEW",
                        sent.id() + " " + sent.key(), wire.getBytes(StandardCharsets.UTF_8));
                if (producer.send(trace).getSendStatus() != SendStatus.SEND_OK) {
                    throw new IllegalStateException("Trace was not stored for " + sent.id());
                }
                System.out.println("TRACE_STORED " + sent.id() + " " + sent.key());
            }
        } finally {
            producer.shutdown();
        }
    }

    private static String encode(TraceType type, Event event, String group, TraceBean bean) {
        TraceContext context = new TraceContext();
        context.setTraceType(type);
        context.setAccessChannel(AccessChannel.LOCAL);
        context.setTraceBeans(List.of(bean));
        context.setTimeStamp(event.time().toEpochMilli());
        context.setGroupName(group);
        context.setRegionId("ReviewCourse");
        context.setRequestId(bean.getMsgId());
        return TraceDataEncoder.encoderFromContextBean(context).getTransData();
    }
}
