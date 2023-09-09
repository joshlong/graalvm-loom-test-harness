package com.example.harness;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.util.Assert;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(HarnessConfigurationProperties.class)
public class HarnessApplication {

    public static void main(String[] args) {
        SpringApplication.run(HarnessApplication.class, args);
    }

    @Bean
    ApplicationRunner runner(Harness harness, HarnessConfigurationProperties configurationProperties) {
        return args -> {
            var concurrency = 10;
            var threads = 1000;
            var analysis = new HashMap<String, Harness.AnalysisResults>();
            for (var e : Map.of("traditional", false, "loom", true).entrySet()) {
                var testDescription = e.getKey();
                var useLoom = e.getValue();
                var log = new File(configurationProperties.logs(), testDescription);

                var build = new File(configurationProperties.root(), "build");
                if (build.exists()) {
                    System.out.println("deleting " + build.getAbsolutePath());
                    FileSystemUtils.deleteRecursively(build);
                }
                var apacheBenchConfiguration = new Harness.ApacheBenchConfiguration(log, threads, concurrency);
                analysis.put(testDescription, harness.test(useLoom, apacheBenchConfiguration));
            }
            System.out.println(analysis);
        };
    }

    @Bean
    RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    @Bean
    ExecutorService service() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    Harness harness(ApplicationEventPublisher publisher,
                    ExecutorService executorService, RestTemplate template,
                    HarnessConfigurationProperties properties) {
        Assert.state(properties.root().exists(), "the codebase root does not exist");

        var logs = properties.logs();
        FileSystemUtils.deleteRecursively(logs);

        Assert.state(logs.exists() || logs.mkdirs(), "the logs root not exist " +
                                                     "and could not be created");
        return new Harness(publisher, executorService, properties.root(), logs, template);
    }
}


@ConfigurationProperties(prefix = "harness")
record HarnessConfigurationProperties(File root, File logs) {
}

class Harness {

    private final ApplicationEventPublisher publisher;
    private final ExecutorService executorService;

    private final File root;

    private final File logRoot;
    private final File properties;

    private final RestTemplate restTemplate;

    private final File errors;
    private final File output;

    private final ProcessBuilder.Redirect stderr;
    private final ProcessBuilder.Redirect stdout;

    Harness(ApplicationEventPublisher publisher, ExecutorService executor, File file, File logRoot, RestTemplate restTemplate) {
        this.publisher = publisher;
        this.executorService = executor;
        this.root = file;
        this.logRoot = logRoot;
        this.restTemplate = restTemplate;
        Assert.state(this.root.exists(), "the code must exist");
        this.properties = new File(this.root, "src/main/resources/application.properties");
        this.errors = new File(this.logRoot, "errors");
        this.output = new File(this.logRoot, "output");
        this.stderr = ProcessBuilder.Redirect.appendTo(this.errors);
        this.stdout = ProcessBuilder.Redirect.appendTo(this.output);
    }

    private void transformProperties(boolean loom) throws Exception {
        var properties = new Properties();
        try (var fin = new FileInputStream(this.properties)) {
            properties.load(fin);
        }
        properties.setProperty("spring.threads.virtual.enabled", Boolean.toString(loom));
        try (var out = new FileWriter(this.properties)) {
            properties.store(out, "modifying the properties");
        }
    }

    private void compile() throws Exception {
        var process = new ProcessBuilder()
                .command(new File(this.root, "gradlew").getAbsolutePath(), "nativeCompile")
                .directory(this.root)
                .inheritIO()
                .redirectOutput(this.stdout)
                .redirectError(this.stderr)
                .start();
        var finished = process.waitFor();
        Assert.state(finished == 0, "the process should complete successfully");
    }

    record ApacheBenchConfiguration(File log, int threads, int concurrency) {
    }

    AnalysisResults test(boolean loom, ApacheBenchConfiguration apacheBenchConfiguration) throws Exception {
        transformProperties(loom);
        compile();
        executorService.submit(() -> {
            try {
                launch();
            } catch (Exception e) {
                throw error(e);
            }
        });


        pollUntilApplicationHealthIs(Health.UP);

        var log = apacheBenchConfiguration.log(); // new File(this.logRoot, "ab-" + (loom ? "loom" : "traditional"));
        Assert.state(!log.exists() || log.delete(), "the file should not exist at first");

        try (var fout = new FileWriter(log)) {
            fout.append(System.lineSeparator());
        }

        var maxRuns = 10;
        for (var i = 0; i < maxRuns; i++)
            ab(log, apacheBenchConfiguration.threads(), apacheBenchConfiguration.concurrency());

        shutdownNativeProcess();

        pollUntilApplicationHealthIs(Health.DOWN);

        return analyzeApacheBenchResults(log);


    }

    enum Health {UP, DOWN}

    private void pollUntilApplicationHealthIs(Health health) throws Exception {
        while (true) {
            Thread.sleep(500);
            var status = false;
            try {
                var responseEntity = this.restTemplate.exchange(RequestEntity
                                .get("http://localhost:8080/actuator/health")
                                .build(),
                        new ParameterizedTypeReference<Map<String, String>>() {
                        });
                status = (responseEntity.getStatusCode().is2xxSuccessful());
            } ///
            catch (Throwable throwable) {
                // error(throwable);
            }
            var match = status ? Health.UP : Health.DOWN;
            if (match.equals(health))
                return;
        }

    }

    record AnalysisResults(float allTestsRunAverage, float timePerRequestAverage,
                           float requestsPerSecondAverage) {
    }

    private AnalysisResults analyzeApacheBenchResults(File abLogs) throws Exception {
        var lines = Files.readAllLines(abLogs.toPath(), Charset.defaultCharset());

        var timeForAllTests = lines
                .stream()
                .filter(line -> line.contains("Time taken for tests"))
                .map(line -> line.split(":")[1].trim().split(" ")[0])
                .map(String::strip)
                .map(Float::parseFloat);

        var timePerRequest = lines
                .stream()
                .filter(line -> line.contains("Time per request") &&
                                line.contains("across all concurrent requests"))
                .map(line -> line.split(":")[1].trim().split(" ")[0])
                .map(String::strip)
                .map(Float::parseFloat);

        var rps = lines
                .stream()
                .filter(line -> line.contains("Requests per second"))
                .map(line -> line.split(":")[1].trim().split(" ")[0])
                .map(String::strip)
                .map(Float::parseFloat);


        return new AnalysisResults(findMean(timeForAllTests), findMean(timePerRequest), findMean(rps));


    }

    private float findMean(Stream<Float> floatStream) {
        var statistics = floatStream.collect(FloatSummary::new, FloatSummary::accept, FloatSummary::combine);
        Assert.state(statistics.count != 0, "Cannot compute mean for empty stream");
        return statistics.sum / statistics.count;
    }

    private static class FloatSummary {
        float sum = 0.0f;
        int count = 0;

        void accept(Float value) {
            sum += value;
            count++;
        }

        void combine(FloatSummary other) {
            sum += other.sum;
            count += other.count;
        }
    }

    private void shutdownNativeProcess() {
        var requestEntity = RequestEntity
                .post("http://localhost:8080/actuator/shutdown")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LinkedMultiValueMap<>());
        var shutdownResponse =
                this.restTemplate.exchange(requestEntity, String.class);
        Assert.state(shutdownResponse.getStatusCode().is2xxSuccessful(),
                "the service has been shutdown");

    }

    private void ab(File log, int threads, int concurrency) throws Exception {
        var cmd = new String[]{"ab", "-n " + threads, "-c " + concurrency, "http://localhost:8080/customers"};
        var redirect = ProcessBuilder.Redirect.appendTo(log);
        var process = new ProcessBuilder()
                .command(cmd)
                .redirectError(redirect)
                .redirectOutput(redirect)
                .directory(this.root)
                .start();
        Assert.state(process.waitFor() == 0, "the ab process did not exit successfully");
    }

    private void launch() throws Exception {
        var directory = new File(this.root, "build/native/nativeCompile/");
        var binary = new File(directory, "service");
        Assert.state(directory.exists() && binary.exists(), "the binary must exist");
        var process = new ProcessBuilder()
                .directory(directory)
                .command(binary.getAbsolutePath())
                .inheritIO()
                .redirectOutput(this.stdout)
                .redirectError(this.stderr)
                .start();
        Assert.state(process.waitFor() == 0, "the process must start and stop normally");
    }

    private static RuntimeException error(Throwable throwable) {
        System.out.println("oops! " + throwable.getMessage());
        return new RuntimeException(throwable);
    }
}