'use strict'

let async = require('async')

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

sqsbarrel.sendToQueue = function ( division, entity, message, callback ) {
	let self = this
	if ( !self.queues[ division ][ entity ] )
		self.createQueue( division, entity, false, (err) => {
			if (err) return callback(err)
			self.sqs.sendMessage({ MessageBody: message, QueueUrl: self.queues[ division ][ entity ], DelaySeconds: 0 }, callback )
		} )
	else
		self.sqs.sendMessage({ MessageBody: message, QueueUrl: self.queues[ division ][ entity ], DelaySeconds: 0 }, callback )
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

sqsbarrel.deleteQueue = function (QueueUrl, callback) {
	this.sqs.deleteQueue( { QueueUrl: QueueUrl }, callback )
}

sqsbarrel.createQueue = function ( division, entity, listen, callback ) {
	let self = this

	if (!self.queues[ division ]) self.queues[ division ] = { }

	if ( self.queues[ division ][ entity ] ) return callback()

	let queueName = hy( division + '.' + entity )

	let fns = []
	fns.push( (cb) => {
		self.sqs.createQueue( { QueueName: queueName }, cb )
	} )
	fns.push( (data, cb) => {
		self.queues[ division ][ entity ] = data.QueueUrl

		self.logger.harconlog( null, 'SQS queue is made.', { queueName: queueName, queue: data }, 'info' )

		cb( null, data.QueueUrl )
	} )
	fns.push( (QueueUrl, cb) => {
		return self.purgeQueues ? self.sqs.purgeQueue( { QueueUrl: QueueUrl }, () => { cb( null, QueueUrl ) } ) : cb( null, QueueUrl )
	} )
	fns.push( (QueueUrl, cb) => {
		cb( null, self.createListener( QueueUrl, self.newQueueListener( QueueUrl ) ) )
	} )

	async.waterfall( fns, callback )
}

sqsbarrel.extendedInit = function ( config, callback ) {
	let self = this

	return new Promise( (resolve, reject) => {
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
				messageData.Messages.forEach( (message) => {
					try {
						let comm = JSON.parse( message.Body )

						let reComm = Communication.importCommunication( comm.comm )
						let reResComm = comm.response ? (comm.responseComms.length > 0 ? Communication.importCommunication( comm.responseComms[0] ) : reComm.twist( self.systemFirestarter.name, comm.err ) ) : null

						let interested = (!reResComm && self.matching( reComm ).length !== 0) || (reResComm && self.matchingResponse( reResComm ).length !== 0)

						if ( !interested )
							return self.sqs.changeMessageVisibility({ QueueUrl: QueueUrl, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }, function (err) { if (err) self.logger.harconlog(err) } )

						self.sqs.deleteMessage({ QueueUrl: QueueUrl, ReceiptHandle: message.ReceiptHandle }, (err) => { if (err) self.logger.harconlog(err) } )
						self.innerProcessAmqp( comm )
					} catch (err) { self.logger.harconlog(err) }
				} )
			}
		}

		self.timeout = config.timeout || 0

		self.connect( Proback.handler( callback, resolve, reject ) )
	} )
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

sqsbarrel.newDivision = function ( division, callback ) {
	return Proback.quicker( 'ok', callback )
}

sqsbarrel.removeEntity = function ( division, context, name, callback) {
	return Proback.quicker( 'ok', callback )
}

sqsbarrel.newEntity = function ( division, context, name, callback) {
	var self = this

	return new Promise( (resolve, reject) => {
		async.series( [
			(cb) => {
				if (division)
					self.createQueue( division, name, true, cb )
				else cb()
			},
			(cb) => {
				if (context)
					self.createQueue( division, context, true, cb )
				else cb()
			}
		], Proback.handler( callback, resolve, reject ) )
	} )
}

sqsbarrel.innerProcessAmqp = function ( comm ) {
	let self = this

	self.logger.harconlog( null, 'Received from bus...', comm, 'silly' )

	let realComm = Communication.importCommunication( comm.comm )
	realComm.nodeSeqNo = comm.nodeSeqNo || 1

	if ( !comm.response ) {
		// console.log( comm.callback )
		if ( comm.callback )
			realComm.callback = function () { }
		self.logger.harconlog( null, 'Request received from bus...', realComm, 'silly' )
		self.parentIntoxicate( realComm )
	} else {
		if ( self.messages[ comm.id ] ) {
			realComm.callback = self.messages[ comm.id ].callback
			delete self.messages[ comm.id ]
		}
		let responses = comm.responseComms.map(function (c) { return Communication.importCommunication( c ) })

		self.parentAppease( realComm, comm.err ? new Error(comm.err) : null, responses )
	}
}

sqsbarrel.parentAppease = sqsbarrel.appease
sqsbarrel.appease = function ( comm, err, responseComms ) {
	let self = this
	if ( !comm.expose && self.isSystemEvent( comm.event ) )
		return this.parentAppease( comm, err, responseComms )

	if ( !self.queues[ comm.division ] )
		return self.logger.harconlog( new Error('Division is not ready yet: ' + comm.division) )

	let packet = JSON.stringify( { id: comm.id, comm: comm, nodeSeqNo: self.nodeSeqNo, err: err ? err.message : null, response: true, responseComms: responseComms || [] } )

	self.logger.harconlog( null, 'Appeasing...', {comm: comm, err: err ? err.message : null, responseComms: responseComms}, 'silly' )
	try {
		self.sendToQueue( comm.sourceDivision, comm.source, packet, (err, data) => {
			if (err) self.logger.harconlog(err)
		} )
	} catch (err) {
		self.logger.harconlog( err )
	}
}

sqsbarrel.parentIntoxicate = sqsbarrel.intoxicate
sqsbarrel.intoxicate = function ( comm ) {
	let self = this
	if ( self.isSystemEvent( comm.event ) ) return this.parentIntoxicate( comm )

	if ( !self.queues[ comm.division ] )
		return self.logger.harconlog( new Error('Division is not ready yet: ' + comm.division) )

	self.logger.harconlog( null, 'Intoxicating to bus...', comm, 'silly' )

	if ( self.messages[ comm.id ] )
		return self.logger.harconlog( new Error('Duplicate message delivery!'), comm.id )

	if ( comm.callback )
		self.messages[ comm.id ] = { callback: comm.callback, timestamp: Date.now() }

	let packet = JSON.stringify( { id: comm.id, comm: comm, nodeSeqNo: self.nodeSeqNo, callback: !!comm.callback } )
	try {
		self.sendToQueue( comm.division, entityOf( comm.event ), packet, (err, data) => {
			if (err) self.logger.harconlog(err)
		} )
	} catch (err) {
		self.logger.harconlog( err )
	}
}

sqsbarrel.clearClearer = function ( ) {
	if ( this.cleaner ) {
		clearInterval( this.cleaner )
		this.cleaner = null
	}
}

sqsbarrel.connect = function ( callback ) {
	let self = this

	self.sqs = new AWS.SQS( { apiVersion: '2012-11-05' } )

	self.logger.harconlog( null, 'SQS creation is made.', {}, 'warn' )

	self.clearClearer()
	if ( self.timeout > 0 ) {
		self.cleaner = setInterval( function () {
			self.cleanupMessages()
		}, self.timeout )
	}

	callback()
}

sqsbarrel.extendedClose = function ( callback ) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.finalised = true
		self.clearClearer()

		let fns = []
		if (self.purgeQueues)
			Object.keys(self.queues).forEach( function (domain) {
				Object.keys( self.queues[domain] ).forEach( function (entity) {
					fns.push( function (cb) { self.sqs.purgeQueue( { QueueUrl: self.queues[domain][entity] }, (err) => { cb( err ) } ) } )
				} )
			} )
		if (self.deleteQueues)
			Object.keys(self.queues).forEach( function (domain) {
				Object.keys( self.queues[domain] ).forEach( function (entity) {
					fns.push( function (cb) { self.deleteQueue( self.queues[domain][entity], (err) => { cb(err) } ) } )
				} )
			} )
		async.series( fns, () => {
			Proback.resolver('ok', callback, resolve)
		} )
	} )
}

module.exports = SqsBarrel
