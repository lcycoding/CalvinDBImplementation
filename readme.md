# Project Goal
This Final project is aimed for the course **Cloud Database** in NTHU.
Implementing part of the paper [Calvin: Fast Distributed Transactions for Partitioned Database Systems](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf)

## Concept
  In Calvin, we partitioned the replica of the database. Let each partition hold the data section they should manage.
  After that, we implement a polling agent collecting the data info to `Client`.

## Work Flow
    1. Read/WriteSet Analysis
    2. Perform local reads
    3. Serve remote reads
    4. Collect remote read results
    5. Perform transaction logic and writes

## Updated Class
    1. Naive_StoredProcedure -> Calvin_StoredProcedure
    2. CacheMgr -> Calvin_CacheMgr
    3. Setting changes in ConnectionMgr

## Later On
