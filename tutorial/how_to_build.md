If you are building this project from source, you can try the following

```
git clone https://github.com/spark-redshift-community/spark-redshift.git
```

```
cd spark-redshift
```

```
./build/sbt -v compile
```

```
./build/sbt -v package
```

To run the test

```
./build/sbt -v test
```

To run the integration test

For the first time, you need to set up all the evnironment variables to connect to Redshift (see https://github.com/spark-redshift-community/spark-redshift/blob/master/src/it/scala/io/github/spark_redshift_community/spark/redshift/IntegrationSuiteBase.scala#L54).

```
./build/sbt -v it:test
```
