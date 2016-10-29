SQS plugin for harcon


[harcon](https://github.com/imrefazekas/harcon) is a microservice solution for NodeJS/Browser giving superior abstraction layer for interoperability between entities in a highly structured and fragmented ecosystem.
It allows you to design and implement complex workflows and microservices where context and causality of messages are important.


[harcon-sqs](https://github.com/imrefazekas/harcon-sqs) is a plugin to give a transport layer to [harcon](https://github.com/imrefazekas/harcon) exchanging messages over [Amazon SQS](https://aws.amazon.com/sqs/).



## Installation

```javascript
npm install harcon harcon-sqs --save
```



## Usage

```javascript
var Harcon = require('harcon');
var Sqs = require('harcon-sqs');

var sqsConfig = {
	maxNumberOfMessages: 10, // number of how many messages are allowed to be downloaded from the queue
	scheduleInterval: 50, // schedule of message downloading
	errInterval: 5000, // schedule used when download process is hit by an error
	purgeQueues: false, // optional. If true, removes all messages when harcon starts and exists.
	deleteQueues: false, // optional. If true, deletes queues when harcon exists.
	accessKeyId: ..., // AWS access key
	secretAccessKey: ..., // AWS secret access key
	region: ... // AWS region
};
var harcon = new Harcon( { Barrel: Sqs.Barrel, barrel: sqsConfig }, function(err){
} );
```
