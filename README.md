# CS4224 Wholesale Project (Cassandra Implementation)
## Setting up the database
1) The following instructions assume that:
* a Cassandra cluster has been set up.

2) In `cassandra.yaml` of each Cassandra cluster node, set the values of the variables to the one shown below.
Restart the Cassandra node after `cassandra.yaml` file is updated.
```
read_request_timeout_in_ms: 60000
range_request_timeout_in_ms: 60000
write_request_timeout_in_ms: 60000
counter_write_request_timeout_in_ms: 5000
cas_contention_timeout_in_ms: 1000
truncate_request_timeout_in_ms: 60000
request_timeout_in_ms: 60000
```

3) Log into one of the Cassandra cluster node.

4) Upload the files under `scripts/load_data` directory of the project root directory to the cluster node.

5) Create a file called `create_table.cql` on the cluster node. Copy the content of `create_table_workload_A.cql` or
`create_table_workload_B.cql` into `create_table.cql` accordingly based on the type of workload to run. For example, if
you want to run workload A, copy `create_table_workload_A.cql` into `create_table.cql`.

6) In the directory where `scripts/load_data` directory was uploaded, run `./load_data.sh`. The script:
* downloads `project_files_4.zip`.
* creates additional files from the data files in `project_files_4` for some tables in the keyspace.
* creates the `wholesale` keyspace.
* load data into the `wholesale` keyspace.
   

## Compilation of Client Program
The following compilation has been tested on macOS.
1) Install the following software on your local machine:
* Gradle (version 7.2)
* Java (version 11.0.12)
    * Make sure that `JAVA_HOME` variable is pointing to the installed Java 11 directory.

2) To compile, run the following command in the project root directory.
```
gradle shadowJar
```
* The compiled jar file can be found in the `build/libs` directory.


## Execution
The following instructions assumes that:
* The keyspace `wholesale` has been created in the Cassandra cluster following the instructions mentioned in
[set up database](#setting-up-the-database) section.

### Usage of jar file
```
usage: Wholesale-Cassandra-1.0-SNAPSHOT-all.jar
 -f,--fileName <arg>      Name of query file
 -i,--ip <arg>            IP address of cassandra cluster
 -k,--keyspace <arg>      Keyspace name
 -l,--logFileName <arg>   Name of log file
 -p,--port <arg>          Port of cassandra cluster
 -t,--task <arg>          Type of task: transaction or dbstate
```
* Required arguments for all type of tasks: `-t`
* Required arguments for processing input transaction file: `-f, -k`
* Required arguments for computing final state of database: `-k`
* Other arguments are optional.
* Default value of optional argument:
    * `-l`: `out.log`
    * `-i`: `localhost`
    * `-p`: `9042`


### How to run the jar file for processing input transaction file
1) Example 1: Runs the jar file on the cluster node that runs the Cassandra instance:
```
java -jar Wholesale-Cassandra-1.0-SNAPSHOT-all.jar -t transaction -f xact_files_B/0.txt -k wholesale -l 0-out.log 1> out/workload_B/0.out 2> out/workload_B/0.err
```

2) Example 2: Runs the jar file on a remote machine (i.e. not on the cluster node that runs the Cassandra instance)
```
java -jar Wholesale-Cassandra-1.0-SNAPSHOT-all.jar -t transaction -f xact_files_B/0.txt -k wholesale -l 0-out.log -i [IP address of Cassandra node] 1> out/workload_B/0.out 2> out/workload_B/0.err
```


### How to run the jar file for computing the final state of the database
The final state of the database is saved to a file called `dbstate.csv`.
1) Example 1: Runs the jar file on the cluster node that runs the Cassandra instance:
```
java -jar Wholesale-Cassandra-1.0-SNAPSHOT-all.jar -t dbstate -k wholesale
```

2) Example 2: Runs the jar file on a remote machine.
```
java -jar Wholesale-Cassandra-1.0-SNAPSHOT-all.jar -t dbstate -k wholesale -i [IP address of Cassandra node]
```

## Running 40 clients simultaneously
A few Bash scripts have been created for running 40 clients simultaneously. The scripts are `prep.sh`, `launch.sh`, and
`run-clients.sh`. They can be found under `scripts/profiling` of the project root directory.

The scripts assume that:
* there are 5 Cassandra cluster nodes.
* `tmux` is installed on those nodes.

### Steps
1) Upload the scripts in `scripts/profiling` to one of the Cassandra cluster node.
2) Create a directory in the `/temp` directory of the cluster node, e.g. `mkdir -p /temp/cs4224o/profiling/cassandra`
3) In the created directory, create a directory called `profiling_files`.
4) Upload the compiled jar file to the `profiling_files` directory.
5) Copy the provided transaction files directories (`xact_files_A` and `xact_files_B`) into the `profiling_files` directory.
6) Copy `run-clients.sh`into the `profiling_files` directory.
7) `cd` to the parent directory of the `profiling_files` directory.
8) Place `prep.sh`, `launch.sh`, and `gather_outputs.sh` in the current directory.
9) In `prep.sh`, substitute the `servers` variable with the list of hostnames of other nodes to run the clients on.
10) Run `prep.sh` to send the `profiling_files` archive to the group of Cassandra cluster nodes.
11) In `launch.sh`, substitute the `servers` variable with the list of hostnames of other nodes to run the clients on.
12) Run `launch.sh` to launch 40 clients simultaneously on the 5 Cassandra cluster nodes.
* The script launches 8 client instances at each node, following the server requirement S(i mod 5). For example, clients
0, 5, 10, 15, 20, 25, 30, 35 execute on `xnc40`, clients 1, 6, 11, 16, 21, 26, 31, 36 execute on `xcnc41` and so on.
* The script runs `run.sh` in `profiling_files` subdirectory of the current directory on every node. 
```
Usage: launch <keyspace name> <workload_type>
keyspace_name: Name of keyspace for the workload
workload_type: A or B

# e.g.
./launch.sh wholesale A
```
12) Run `tmux ls` to check the status of the running clients on the current node.
13) Add the following to your `~/.bash_profile` on the cluster node to check the status of running clients on the other
    nodes. Run `source ~/.bash_profile` to reload the Bash profile.
```
# Replace the list of servers (xcnc4{1..4}) accordingly.
alias checkstatus='for server in xcnc4{1..4}; do ssh $server "tmux ls"; done'
```
14) Run `checkstatus` to check the status of the running clients on other nodes.
15) Once the running clients finish, you can run `gather_outputs.sh` to gather all the output files back to current
    node.
* Replace the list of nodes in `gather_outputs.sh` before running the script.

## Consolidating statistics
* A python script called `stats_cal.py` is provided in `scripts/profiling` under the project root directory.
* Run the script to consolidate `.err` files from all the servers.
```
Usage: python3 stats_cal.py --i [input_dir] --o [output_dir]
input_dir: a directory containing .err files generated from clients. Each .err file should be in the format of [i].err,
where i is from 0 to 39 inclusive.
output_dir: a directory to contain the consolidated stats files.
```
* The output files include `clients.csv`, `throughput.csv`, and .csv files of statistics per transaction type of each client.

## Additional Details
1) The `cassandra_conf` directory of the project root directory contains `cassandra.yaml` files used for each of the
allocated nodes (xcnc40-44). The suffix of the filename (e.g. `_40`) corresponds to the `cassandra.yaml` file on the node
with the node id xcnc`[suffix]`. For example, `cassandra_40.yaml` file is the `cassandra.yaml` file on `xcnc40`. 