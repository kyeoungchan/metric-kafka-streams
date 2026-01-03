package com.example.metrickafkastreams;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

public class MetricStreams {

    private static KafkaStreams streams;

    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties props = new Properties();

        // 스트림즈 애플리케이션은 애플리케이션 아이디 값을 기준으로 병렬처리하기 때문에 기존에 작성되지 않은 애플리케이션 아이디를 사용한다.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "metric-streams-application");

        // 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");

        /* 스트림 처리를 위해 메시지 키와 메시지 값의 역직렬화, 직렬화 방식을 지정한다.
         * 스트림즈 애플리케이션에서는 데이터를 처리할 때 메시지 키 또는 메시지 값을 역직렬화하여 사용하고 최종적으로 데이터를 토픽에 넣을 때는 직렬화해서 데이터를 저장한다. */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 스트림 토폴로지를 정의하기 위한 StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // metric.all 토픽으로부터 KStream 객체를 만들기 위해 StreamBuilder의 stream() 메서드 사용
        KStream<String, String> metrics = builder.stream("metric.all");

        /* Named.as(): split()으로 생성되는 모든 하위 노드들의 이름 앞에 metric- 이라는 prefix를 붙여라
         * split(): 메시지 값을 기준으로 분기처리하기 위해 MetricJsonUtils를 통해 JSON 데이터에 적힌 메트릭 종류 값을 토대로 KStream을 두 갈래로 분기한다.
         * Branched.as(): KStream을 다시 사용해야 할 때, 각 branch의 논리적 이름을 지정하고 반환 */
        Map<String, KStream<String, String>> metricBranch = metrics.split(Named.as("metric-"))
                .branch(
                        (key, value) -> MetricJsonUtils.getMetricName(value).equals("cpu"),
                        Branched.as("cpu")
                )
                .branch(
                        (key, value) -> MetricJsonUtils.getMetricName(value).equals("memory"),
                        Branched.as("memory")
                )
                // 위에서 정의한 branch 조건에 하나도 안 걸리는 데이터는 버려라
                .noDefaultBranch();

        KStream<String, String> cpuMetricStream = metricBranch.get("metric-cpu");
        KStream<String, String> memoryMetricSteam = metricBranch.get("metric-memory");

        // 각 데이터에 맞는 각 토픽으로 전달
        cpuMetricStream.to("metric.cpu");
        memoryMetricSteam.to("metric.memory");

        // 분기처리된 데이터 중 전체 CPU 사용량이 50%가 넘어갈 경우를 필터링하기 위해 filter() 메서드 사용
        KStream<String, String> filteredCpuMetric = cpuMetricStream
                .filter((key, value) -> MetricJsonUtils.getTotalCpuPercent(value) > 0.5);

        /* 전체 CPU 사용량의 50%가 넘는 데이터의 메시지 값을 모두 전송하는 것이 아니라 호스트 이름과 timestamp 값만 필요하므로 2개의 값을 조합하는 MetricJsonTuils의 getHostTimeStamp() 메서드를 호출하여 변환된 형태로 받는다.
         * 이 데이터는 to() 메서드에 정의된 metric.cpu.alert 토픽으로 전달된다. */
        filteredCpuMetric.mapValues(MetricJsonUtils::getHostTimestamp).to("metric.cpu.alert");

        // StreamBuilder 인스턴스로 정의된 토폴로지와 스트림즈 설정값을 토대로 KafkaStreams 인스턴스를 생성하고 실행한다.
        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    static class ShutdownThread extends Thread {
        public void run() {
            // Kafka Streams의 안전한 종료를 위해 셧다운 훅을 받을 경우 close() 메서드를 호출하여 안전하게 종료한다.
            streams.close();
        }
    }
}
