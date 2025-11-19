Pipeline Documentation: utility_batch_process_pipeline



 Overview

 Pipeline name: utility_batch_process_pipeline
 Number of activities: 3



 Activities

 1. Get Business Systems To Run
 Type: `Lookup`
 Description: Retrieve all the parameter attributes from the fabric warehouse stored procedure to return configuration details; server, database, source file names, source, target, etc

 2. Run Business Processes
 Type: `ForEach`
 Description: All the rows indicating jobs to run would be collected and passed to the ForEach loop, which in turn process them row by row

 3. Set Next Frequency Runs
 Type: `SqlServerStoredProcedure`
 Description: Though named so, but this can run for as many times the module is processed.

![utility_batch_process_pipeline](utility_batch_process_pipeline.png)
