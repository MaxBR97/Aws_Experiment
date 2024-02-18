Liam Habani & Max Brener
_208850529

Instructions:

1.Set the AWS credentials as environment variables for the local app 'App'.
2.Export 2 Jar files of the entire current root folder, once with 'Manager' being the main method of the program, and once with 'Worker' being the main method for the jar file.
3.Rename the exported Jar files of step 2 to 'Manager.jar' and 'Worker.jar' respectively, and make sure they are placed in the root folder of the project (same folder as this readme.md file).
4.


Instance types used:
Manager - ami-0c7217cdde317cfec,  T2_MEDIUM
Worker - ami-0c7217cdde317cfec, T2_LARGE

The local app receives parameters inputPath1, inputPath2, ... ,inputPathN, outputPath1, ... , outputPathN taskToWorkersRatio [terminate] and procceses the input files specified, and writes the results to the output files mentioned respectively. taskToWorkersRatio specifies the ratio between the tasks that are given to process simultaniously to the number of workers. "terminate" is an optional, in case terminating all of the ec2's is wished.

Those are the SQS messages protocols between clients to the manager and between the manager to it's workers.

Protocols:

SQS "worker task" message format (manager appends, worker consumes):
    "task <S3 key to input file>"

SQS "task finished" message format (worker appends, manager consumes):
    "finished <S3 key to summary file>"

SQS "input message" message format (local app appends, manager consumes):
    "input <client key> <S3 file path>"

SQS "summary ready" message format (manager appends, local app consumes):
    "summary <client key> <S3 file path>

* client key is a unique identifier of the client
* different SQS queues for manager-worker and client-manager