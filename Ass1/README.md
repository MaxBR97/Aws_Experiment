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