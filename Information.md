# Final Project
In this assignment, you have to implement a deterministic distributed database system in the `VanillaDD` project.

## Steps
To finish this assignment, you need to

1. Fork the final project repository
2. Trace the code of naive implementation in the `vanilladb-dd` project yourself
3. Implement the following modules in the `vanilladb-dd` project
    - `CalvinCacheMgr` in `org.vanilladb.dd.cache.calvin`
    - `CalvinScheduler` in `org.vanilladb.dd.schedule.calvin`
    - `CalvinStoredProcedure` in `org.vanilladb.dd.schedule.calvin`
    - `CalvinStoredProcedureTask` in `org.vanilladb.dd.server.task.calvin`
4. Implement the `SchemaBuilder`, `TestbedLoader`, `Microbenchmark` stored procedures, and your `StoredProcedureFactory` for your Calvin implementation in the `benchmark-new` project
    - These stored procedures and factory should be in `netdb.software.benchmark.procedure.vanilladddb.calvin`
    - For `TestbedLoader` procedure, let each server just load their **own partition**, not the whole date set
5. Run scalability experiments
6. Write a report

## Implement Calvin

Calvin is a deterministic distributed database system prototype implemented by a research team in Yale. The paper, "Calvin: Fast Distributed Transactions for Partitioned Database Systems" in SIGMOD’12, describes how whole Calvin system works.

There are three main packages in `vanilladb-dd` you have to notice:
- `org.vanilladb.dd.cache`
- `org.vanilladb.dd.schedule`
- `org.vanilladb.dd.server.task`

They represent the corresponding modules in the project:
- Cache Layer
- Scheduler
- Store Procedure Task

When the communication module receives a request, the request will be passed to the scheduler. A scheduler is a dedicated thread that requests locks for a transaction and creates a stored procedure task for it, which will be executed in another thread. A scheduler follows these steps to handle a request:

1. Receive the request
2. Log the request
3. Create a stored procedure
4. Prepare the stored procedure
5. Request locks in the conservative CCMgr
6. Hand over the task to the TaskMgr for execution in another thread

After a stored procedure dispatched to a thread, it first gets the locks it requested in the scheduler. Then execute the transaction in **the Calvin way**. For more information how a Calvin stored procedure executes, you can read the section 3.2 of the Calvin paper.

Cache layer is also very important. It is a interface between `vanilladb-dd` and `vanilladb-core`. Any request for accessing a table record must be passed to it. It also take the responsibility for caching the remote records, which are sent from other server nodes through Internet.

## Hints

We have already provided a naive implementation, which will execute transactions on all server nodes and there is no different between the results in these nodes. (That is why we call it `Fully-replicated`) You can trace that and think about how to extend to a Calvin implementation.

## Running Experiments

In this assignment, you are asked to run a few experiment to prove your system works. When you run experiments in the different number of servers, there are some settings you must notice:

- Data set size
    - `netdb.software.benchmark.TpccConstants.NUM_ITEMS` in `benchmark_tpcc.propeties`
    - [The number of items] = [the number of servers] x 100000
- Number of partition
    - `org.vanilladb.dd.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS` in `vanilladddb.propeties`
- Server views in communication
    - `org.vanilladb.comm.server.ServerAppl.SERVER_VIEW` in `vanilladbcomm.propeties`
    - You have to fill in the network information of each server here
    - Format: `[ID IP Port]`
- RTE number
    - `netdb.software.benchmark.TestingParameters.NUM_RTES` in `benchmark_tpcc.propeties`
    - Generally, the more servers you have, you need to add more RTEs to approach the limitation of your system

In our experiment, the throughput of running two servers in a single machine is not too much better than running one server in a single machine. To show the benefits of partitions, we **highly recommend** you running each process on different physical machines.

Note: Remember to add your stored procedure factory class name to the `org.vanilladb.dd.schedule.calvin.CalvinScheduler.FACTORY_CLASS` and change the value of `org.vanilladb.dd.server.VanillaDdDb.SERVICE_TYPE` in `vanilladddb.propeties`

## The Report

- How you implement Calvin
- Compare the throughputs between the naive and your implementation
- Running the scalability experiments in at least 3 nodes
    - Please use at least 3 different parameters combination and demonstrate the results
- Anything worth to be mentioned

	Note: There is no strict limitation to the length of your report. Generally, a 2~3 pages report with some figures and tables is fine. **Remember to include all the group members' student IDs in your report.**

## Submission

The procedure of submission is as following:

1. Fork our [Final Project](http://shwu10.cs.nthu.edu.tw/2015-cloud-database/final-project) on GitLab
2. Clone the repository you forked
3. Finish your work and write the report
4. Commit your work, push to GitLab and then open a merge request to submit. The repository should contain
	- *[Project directory]*
	- *[Team Member 1 ID]_[Team Member 2 ID]*_final_project_report.pdf (e.g. 102062563_103062528_final_project_reprot.pdf)

    Note: Each team only need one submission.

## Demo

Due to the complexity of this project, we hope you can come to explain your work face to face. We will announce a demo table after the final exam week. Don't forget to choose the demo time for your team.

If you want to demo earlier, please contact TAs.

## Deadline

Sumbit your work before **2015/06/30 (Tue.) 23:59:59**.
