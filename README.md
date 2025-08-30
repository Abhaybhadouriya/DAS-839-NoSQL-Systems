# Data Synchronization Across Heterogeneous Systems

In this project, you are expected to demonstrate your understanding of data integration
and synchronization across heterogeneous systems. Building on the concepts explored in
Assignment 3, assume that the file `student course grades.csv` has
been redundantly loaded onto at least three heterogeneous data systems.  
The systems must include **MongoDB** and any two or more from the following: **Pig, Hive, or PostgreSQL/MySQL**.

---

## Project Requirements

- Implement methods for reading (`get()`) and updating (`set()`) data independently in each system.
- Define an abstract function `merge()` in each system, which merges with another system using **operation logs (oplogs)** only (not direct access).
- Oplog format should be generic and applicable to any table, not just `student course grades.csv`.

---

## Steps

1. **CRUD Identification**  
   Identify CRUD operations supported by each system (MongoDB, Pig, Hive, PostgreSQL/MySQL).  
   Only consider operations relevant to **reading** and **updating** the `Grade` field for a given `(student-ID, course-ID)` pair.  
   > Note: `(student-ID, course-ID)` form a composite primary key.

2. **Merge Functionality**  
   - Example: `PIG.MERGE(SQL)` merges Pig’s table with PostgreSQL using their oplogs.  
   - The merge must ensure mathematical properties such as:
     - **Associativity**
     - **Commutativity**
     - **Idempotency**
   - Reflect on convergence in different scenarios.

---

## Sample Operation Logs

**Hive Oplog (`oplog.hiveql`)**

- 1 , SET (( SID103 , CSE016 ) , A )
- 2 , GET ( SID103 , CSE016 )
  
**PostgreSQL or MySQL Oplog (`oplog.MySQL`)** 

- 1 , GET ( SID103 , CSE016 )
- 2 , GET ( SID403 , CSE013 )
- 3 , SET (( SID103 , CSE016 ) , B )
  
**MongoDB Oplog(`oplog.mongo`)**

- 1 , SET (( SID101 , CSE026 ) , B )
- 2 , GET ( SID403 , CSE013 )
- 3 , SET (( SID101 , CSE026 ) , A )

- Sample logs and test cases
- Short write-up discussing merge behavior & properties

**Requirement:** Must use **at least 3 heterogeneous systems (including MongoDB)**.

