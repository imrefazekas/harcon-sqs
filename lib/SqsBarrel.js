var AWS = require('aws-sdk')
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN

let Harcon = require('harcon')
let Barrel = Harcon.Barrel
let Communication = Harcon.Communication

function SqsBarrel ( ) { }
SqsBarrel.prototype = new Barrel()
let sqsbarrel = SqsBarrel.prototype

let Proback = require('proback.js')

const SEPARATOR = '-'

function entityOf ( event ) {
	return event.substring( 0, event.lastIndexOf('.') )
}

function hy ( str ) {
	return str.replace(/\./g, SEPARATOR)
}

sqsbarrel.sendToQueue = async function ( division, entity, message ) {
	let self = this
	if ( !self.queues[ division ][ entity ] ) {
		await self.createQueue( division, entity, false)
		return self.sqsSendMessage( message, division, entity )
	} else
		return self.sqsSendMessage( message, division, entity )
}

sqsbarrel.sqsSendMessage = function ( message, division, entity ) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.sqs.sendMessage({ MessageBody: message, QueueUrl: self.queues[ division ][ entity ], DelaySeconds: 0 }, Proback.handler( null, resolve, reject ) )
	} )
}

sqsbarrel.createListener = function ( QueueUrl, Fn ) {
	let self = this
	self.sqs.receiveMessage({ MaxNumberOfMessages: self.maxNumberOfMessages, QueueUrl: QueueUrl }, (err, messageData) => {
		if (err) return self.logger.harconlog(err)

		else if (messageData.Messages)
			Fn(messageData)

		if (!self.finalised)
			setTimeout( () => { self.createListener( QueueUrl, Fn ) }, err ? self.errInterval : self.scheduleInterval )
	} )
}

sqsbarrel.sqsCreateQueue = function (queueName) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.sqs.createQueue( { QueueName: queueName }, Proback.handler( null, resolve, reject ) )
	} )
}

sqsbarrel.sqsDeleteQueue = function (QueueUrl) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.sqs.deleteQueue( { QueueUrl: QueueUrl }, Proback.handler( null, resolve, reject ) )
	} )
}

sqsbarrel.changeMessageVisibility = function (QueueUrl, message) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.sqs.changeMessageVisibility({ QueueUrl: QueueUrl, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }, Proback.handler( null, resolve, reject ) )
	} )
}

sqsbarrel.sqsDeleteMessage = function (QueueUrl, message) {
	let self = this
	return new Promise( (resolve, reject) => {
		try {
			self.sqs.deleteMessage({ QueueUrl: QueueUrl, ReceiptHandle: message.ReceiptHandle }, Proback.handler( null, resolve, reject ) )
		} catch (err) { console.error(err) }
	} )
}

sqsbarrel.sqsPurgeQueue = function (QueueUrl) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.sqs.purgeQueue( { QueueUrl: QueueUrl }, Proback.handler( null, resolve, reject ) )
	} )
}

sqsbarrel.createQueue = async function ( division, entity, listen ) {
	let self = this

	if (!self.queues[ division ]) self.queues[ division ] = { }

	if ( self.queues[ division ][ entity ] ) return 'ok'

	let queueName = hy( division + '.' + entity )

	let data = await self.sqsCreateQueue( queueName )

	self.queues[ division ][ entity ] = data.QueueUrl

	self.logger.harconlog( null, 'SQS queue is made.', { queueName: queueName, queue: data }, 'info' )

	if (self.purgeQueues)
		await self.sqsPurgeQueue( data.QueueUrl )

	self.createListener( data.QueueUrl, self.newQueueListener( data.QueueUrl ) )
}

sqsbarrel.processMessages = function ( messageData, QueueUrl ) {
	let self = this
	return new Promise( async (resolve, reject) => {
		for (let message of messageData.Messages) {
			try {
				let comm = JSON.parse( message.Body )

				let realComm = Communication.importCommunication( comm.comm )
				let reResComm = comm.response ? (comm.responseComms.length > 0 ? Communication.importCommunication( comm.responseComms[0] ) : realComm.twist( self.systemFirestarter.name, comm.err ) ) : null
				let interested = (!reResComm && self.matching( realComm ).length !== 0) || (reResComm && self.matchingResponse( reResComm ).length !== 0)

				if ( !interested )
					return await self.changeMessageVisibility( QueueUrl, message )

				await self.sqsDeleteMessage( QueueUrl, message )
				await self.innerProcessSqs( comm )

				resolve('ok')
			} catch (err) { reject(err) }
		}
	} )
}

sqsbarrel.extendedInit = async function ( config ) {
	let self = this

	self.messages = {}
	self.queues = {}

	self.nodeSeqNo = config.nodeSeqNo || 1
	self.nodeCount = config.nodeCount || 1

	self.maxNumberOfMessages = config.maxNumberOfMessages || 10
	self.purgeQueues = !!config.purgeQueues
	self.deleteQueues = !!config.deleteQueues
	self.scheduleInterval = config.scheduleInterval || 50
	self.errInterval = config.errInterval || 5000

	AWS.config.update({
		accessKeyId: config.accessKeyId,
		secretAccessKey: config.secretAccessKey,
		region: config.region
	})

	self.newQueueListener = function ( QueueUrl ) {
		return function (messageData) {
			self.processMessages( messageData, QueueUrl )
				.then( () => {} )
				.catch( (reason) => { self.logger.harconlog(reason) } )
		}
	}

	self.timeout = config.timeout || 0

	await self.connect( )
}

sqsbarrel.cleanupMessages = function () {
	let self = this

	let time = Date.now()
	for ( let key of Object.keys( self.messages ) ) {
		if ( time - self.messages[key].timestamp > self.timeout ) {
			let callbackFn = self.messages[key].callback
			delete self.messages[ key ]
			callbackFn( new Error('Response timeout') )
		}
	}
}

sqsbarrel.newDivision = async function ( division ) {
	return 'ok'
}

sqsbarrel.removeEntity = async function ( division, context, name) {
	return 'ok'
}

sqsbarrel.newEntity = async function ( division, context, name) {
	var self = this

	if (division)
		await self.createQueue( division, name, true)
	if (context)
		await self.createQueue( division, context, true )

	return 'ok'
}

sqsbarrel.innerProcessSqs = function ( comm ) {
	let self = this

	self.logger.harconlog( null, 'Received from bus...', comm, 'trace' )

	let realComm = Communication.importCommunication( comm.comm )
	realComm.nodeSeqNo = comm.nodeSeqNo || 1

	if ( !comm.response ) {
		if ( comm.callback )
			realComm.callback = function () { }
		self.logger.harconlog( null, 'Request received from bus...', realComm, 'trace' )
		return self.parentIntoxicate( realComm )
	} else {
		if ( self.messages[ comm.id ] ) {
			realComm.callback = self.messages[ comm.id ].callback
			delete self.messages[ comm.id ]
		}
		let responses = comm.responseComms.map(function (c) { return Communication.importCommunication( c ) })

		return self.parentAppease( realComm, comm.err ? new Error(comm.err) : null, responses )
	}
}

sqsbarrel.parentAppease = sqsbarrel.appease
sqsbarrel.appease = async function ( comm, err, responseComms ) {
	let self = this
	if ( !comm.expose && self.isSystemEvent( comm.event ) )
		return this.parentAppease( comm, err, responseComms )

	if ( !self.queues[ comm.division ] )
		throw new Error('Division is not ready yet: ' + comm.division)

	let packet = JSON.stringify( { id: comm.id, comm: comm, nodeSeqNo: self.nodeSeqNo, err: err ? err.message : null, response: true, responseComms: responseComms || [] } )

	self.logger.harconlog( null, 'Appeasing...', {comm: comm, err: err ? err.message : null, responseComms: responseComms}, 'trace' )
	await self.sendToQueue( comm.sourceDivision, comm.source, packet )
}

sqsbarrel.parentIntoxicate = sqsbarrel.intoxicate
sqsbarrel.intoxicate = async function ( comm ) {
	let self = this
	if ( self.isSystemEvent( comm.event ) ) return this.parentIntoxicate( comm )

	if ( !self.queues[ comm.division ] )
		throw new Error('Division is not ready yet: ' + comm.division)

	self.logger.harconlog( null, 'Intoxicating to bus...', comm, 'trace' )

	if ( self.messages[ comm.id ] )
		throw new Error('Duplicate message delivery:' + comm.id )

	if ( comm.callback )
		self.messages[ comm.id ] = { callback: comm.callback, timestamp: Date.now() }

	let packet = JSON.stringify( { id: comm.id, comm: comm, nodeSeqNo: self.nodeSeqNo, callback: !!comm.callback } )
	await self.sendToQueue( comm.division, entityOf( comm.event ), packet )
}

sqsbarrel.clearClearer = function ( ) {
	if ( this.cleaner ) {
		clearInterval( this.cleaner )
		this.cleaner = null
	}
}

sqsbarrel.connect = async function ( ) {
	let self = this

	self.sqs = new AWS.SQS( { apiVersion: '2012-11-05' } )

	self.logger.harconlog( null, 'SQS creation is made.', {}, 'warn' )

	self.clearClearer()
	if ( self.timeout > 0 ) {
		self.cleaner = setInterval( function () {
			self.cleanupMessages()
		}, self.timeout )
	}

	return 'ok'
}

sqsbarrel.extendedClose = async function ( ) {
	let self = this

	self.finalised = true
	self.clearClearer()

	if (self.purgeQueues)
		for (let domain of Object.keys(self.queues) )
			for (let entity of Object.keys(self.queues[domain]) )
				await self.sqsPurgeQueue( self.queues[domain][entity] )
	if (self.deleteQueues)
		for (let domain of Object.keys(self.queues) )
			for (let entity of Object.keys(self.queues[domain]) )
				await self.sqsDeleteQueue( self.queues[domain][entity] )
	return 'ok'
}

module.exports = SqsBarrel
