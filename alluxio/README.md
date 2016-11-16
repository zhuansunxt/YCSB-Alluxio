# Benchmarking Alluxio Metadata RPC Server

## What RPC procedure calls are measured

Since YCSB is designed for database system's bencmark, the approach benchmarking Alluxio's RPC server is to mapping its file operations to database operations, given to the fact that file creation/update/read/deletion shares similar semantics with a key-value database.

In AlluxioClient, we mapped the following RPC procedures to DB operations:

- createFile(path, option) -> insert(dir, file, ...)
- remove(path, option) -> delete(dir, file)
- getStatus(path) -> read(dir, file, ...)
- setAttribute(path, AttrOptions) -> update(dir, file, ...)

## Run a Basic Benchmark

Clone this repository:

```
git clone https://github.com/zhuansunxt/YCSB-Alluxio.git
```

Compile from source:

```
mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

This will only compile the source to the target, and will not run all the checks and tests.

After launching your own Alluxio instance, load Alluxio with initial data:

```
./bin/ycsb load alluxio -P workloads/workload_alluxio_small
```
Run the target workload against your Alluxio master server:

```
./bin/ycsb run alluxio -P workloads/workload_alluxio_small > small_benchmark.log
```



