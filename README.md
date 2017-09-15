# Arrow JMH benchmark
This is a JMH benchmark. To execute it, you have to run

```shell
mvn clean install
java -jar target/benchmarks.jar ".*"
```

The benchmark have some configuration parameters. You can know more about them
by running:

```shell
java -jar target/benchmarks.jar -h
```