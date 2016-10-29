var AWS = require('aws-sdk')
AWS.config.update({
	accessKeyId: process.env.AWS_ACCESS_KEY_ID,
	secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
	region: process.env.AWS_REGION
})

let self = {
	maxNumberOfMessages: 10,
	sqs: new AWS.SQS( { apiVersion: '2012-11-05' } )
}

const SEPARATOR = '-'

function hy ( str ) {
	return str.replace(/\./g, SEPARATOR)
}

function sendToQueue ( division, entity, message, callback ) {
	self.sqs.sendMessage({ MessageBody: message, QueueUrl: self.queues[ division ][ entity ], DelaySeconds: 0 }, callback )
}

function createListener ( division, entity ) {
	self.sqs.receiveMessage({ MaxNumberOfMessages: self.maxNumberOfMessages, QueueUrl: self.queues[ division ][ entity ] }, (err, messageData) => {
		if (err) return console.error(err)

		if (messageData.Messages) {
			messageData.Messages.forEach( (message) => {
				try {
					console.log( '.....', message.Body )
				} catch (err) { console.error(err) }
			} )
			self.sqs.deleteMessageBatch( { Entries: messageData.Messages.map( (m) => { return { Id: m.MessageId, ReceiptHandle: m.ReceiptHandle } } ), QueueUrl: self.queues[ division ][ entity ] }, (err) => { if (err) console.error(err) } )
		}

		setTimeout( () => { createListener( division, entity ) }, 100 )
	} )
}

function createQueue ( division, entity, listen, callback ) {
	if (!self.queues) self.queues = {}
	if (!self.queues[ division ]) self.queues[ division ] = { }

	if ( self.queues[ division ][ entity ] ) return callback()

	let queueName = hy( division + '.' + entity )
	self.sqs.createQueue( { QueueName: queueName }, (err, data) => {
		if (err) return callback(err)

		self.queues[ division ][ entity ] = data.QueueUrl

		if (listen)
			createListener( division, entity )

		callback()
	} )
}

let division = 'divike', entity = 'entike'
createQueue( division, entity, true, (err) => {
	if (err) console.error(err)

	setTimeout( () => {
		let messages = [ 'Hello!', 'Bonjour!', 'Salut!', 'Enchanté' ]
		messages.forEach( (message) => { sendToQueue( division, entity, message, () => {} ) } )

		setTimeout( () => { console.log('------') }, 2000 )
	}, 2000 )
} )
