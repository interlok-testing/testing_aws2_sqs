# AWS Tests
Functional tests for AWS SDK

## SQS
The SQS workflow in the example adapter configuration demonstrates how to configure a producer to an SQS queue (queue1)
as well as a consumer to read from a queue, which subsequently produces to another queue (queue2).

The example can be triggered by calling the Interlok endpoint:

```
/aws2/sqs/
```
