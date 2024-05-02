Liam Habani & Max Brener

Instructions:

1.Set the AWS credentials in .aws/credentials
2.Export 3 Jar files of the entire current root folder,once with 'App' being the main method of the jar, once with 'Manager' being the main method of the jar, and once with 'Worker' being the main method for the jar file.
3.Rename the exported Jar files of step 2 to 'App.jar', 'Manager.jar' and 'Worker.jar' respectively, and make sure they are placed in the root folder of the project (same folder as this readme.md file).
4. Run the program with:
java -jar App.jar inputPath1, inputPath2, ... ,inputPathN, outputPath1, ... , outputPathN taskToWorkersRatio [terminate]

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
------------------------------------------
Instance types used:
Manager - ami-0c7217cdde317cfec,  T2_MEDIUM
Worker - ami-0c7217cdde317cfec, T2_LARGE

Run time on input files 1 to 5 with n=1: 41mins~

* We have thought about security, the credentials are not exposed.

* We have considered the scalability matter. It is developed in a way that it is able to serve many clients with many input files with large sizes. The manager creates Workers according to the mass of work.

* We have tried to examine differet scenarios of unexpected failures and find a solution for them. For example, a message from an SQS is not deleted until the reply message is appended, instead the message is being hidden for up until it is either deleted or the node crashes for some reason. If a crash occurs before a node has finished it's job, the job request in the SQS queue would recover it's visibility within seconds and another node will be able to undertake the task.

* Threads are a good idea when there is some sort of I\O request that needs to be waited for while proccesing another task concurrently.

* We have ran 2 clients at the same time.

* Termination is being considered in our app.

* We are using system abilities to their fullest on the workers by extending the JVM Heap size to the size of the maximum RAM memory in the node.

* All Workers are working hard.

* The Manager isn't doing unnecessary work. Each node has it's own defined task.

* The Manager waits for the workers to finish while still listening to the client's requests. The workers wait for work to be accessible for them while there isn't yet.
