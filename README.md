How to use it
=============

## config

```
$ vi config.yml
```

## Compile

```
$ script/build
```

or

```
$ javac -cp "jars/*" BulkClientDriverUnitTest.java BulkClientDriver.java DriverRunner.java DriverMonitor.java
```

## run

```
$ script/run
```

or

```
$ java -cp ".:jars/*" DriverRunner
```

## run test

```
$ script/runtest
```

or

```
$ java -cp ".:jars/*" BulkClientDriverUnitTest
```
