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

sqsbarrel.sendToQueue = function ( division, message, callback ) {
	this.sqs.sendMessage({ MessageBody: message, QueueUrl: this.queues[division], DelaySeconds: 0 }, function (err, data) {
		console.log('>>>>>>>>>', err, data)
		callback( err, data )
	} )
}

sqsbarrel.createQueue = function ( division, listening, callback ) {
	let self = this

	if (self.queues[division]) return callback()

	self.sqs.createQueue( { QueueName: division }, function (err, data) {
		if (err) return callback(err)

		self.logger.harconlog( null, 'SQS queue is made.', { division: division, queue: data }, 'info' )

		self.queues[division] = data.QueueUrl

		if (listening)
			self.sqs.receiveMessage({ QueueUrl: self.queues[division] }, function (err, data) {
				console.log('<<<<<<<<<', err, data)

				if (err) self.logger.harconlog(err)

				data.Messages.forEach( (message) => {
					try {
						let comm = JSON.parse( message.Body )

						let reComm = Communication.importCommunication( comm.comm )
						let reResComm = comm.response ? (comm.responseComms.length > 0 ? Communication.importCommunication( comm.responseComms[0] ) : reComm.twist( self.systemFirestarter.name, comm.err ) ) : null

						let interested = (!reResComm && self.matching( reComm ).length !== 0) || (reResComm && self.matchingResponse( reResComm ).length !== 0)

						if ( !interested )
							self.sqs.changeMessageVisibility({ QueueUrl: self.queues[division], ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }, function (err, data) { if (err) self.logger.harconlog(err) } )
						else
							self.sqs.deleteMessage({ QueueUrl: self.queues[division], ReceiptHandle: message.ReceiptHandle }, (err) => { self.logger.harconlog(err) } )

						self.innerProcessAmqp( comm )
					} catch (err) { self.logger.harconlog(err) }
				} )
			})

		callback()
	} )
}

sqsbarrel.createIn = function ( division, callback ) {
	this.createQueue( division, true, callback )
}

sqsbarrel.createOut = function ( division, callback ) {
	this.createQueue( division, false, callback )
}

sqsbarrel.extendedInit = function ( config, callback ) {
	let self = this

	return new Promise( (resolve, reject) => {
		self.messages = {}
		self.queues = {}

		self.nodeSeqNo = config.nodeSeqNo || 1
		self.nodeCount = config.nodeCount || 1

		// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
		AWS.config.update({
			accessKeyId: config.accessKeyId,
			secretAccessKey: config.secretAccessKey,
			region: config.region
		})
		self.sqs = new AWS.SQS()

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
	var self = this
	if ( self.queues[division] ) return Proback.quicker( 'ok', callback )
	return new Promise( (resolve, reject) => {
		self.createOut( division, Proback.handler( callback, resolve, reject ) )
	} )
}

sqsbarrel.removeEntity = function ( division, context, name, callback) {
	return Proback.quicker( 'ok', callback )
}

sqsbarrel.newEntity = function ( division, context, name, callback) {
	var self = this

	return new Promise( (resolve, reject) => {
		async.series( [
			(cb) => {
				self.createIn( division, cb )
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
		self.sendToQueue( comm.sourceDivision, packet, (err, data) => {
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
		self.sendToQueue( comm.division, packet, (err, data) => {
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

	self.sqs = new AWS.SQS({region: 'ap-southeast-2', maxRetries: 15})

	self.logger.harconlog( null, 'SQS creation is made.', self.connectURL, 'warn' )

	self.setupDomains( function () {
		if ( callback )
			callback()
	} )

	self.clearClearer()
	if ( self.timeout > 0 ) {
		self.cleaner = setInterval( function () {
			self.cleanupMessages()
		}, self.timeout )
	}
}

sqsbarrel.setupDomains = function ( callback ) {
	let self = this

	let fns = []
	Object.keys(self.queues).forEach( function (domain) {
		fns.push( function (cb) { self.createIn( domain, cb ) } )
	} )
	Object.keys(self.queues).forEach( function (division) {
		fns.push( function (cb) { self.createOut( division, cb ) } )
	} )
	async.series( fns, callback )
}

sqsbarrel.extendedClose = function ( callback ) {
	let self = this
	return new Promise( (resolve, reject) => {
		self.finalised = true
		self.clearClearer()

		let fns = []
		Object.keys(self.queues).forEach( function (domain) {
			fns.push( function (cb) { self.sqs.purgeQueue( { QueueUrl: self.queues[domain] }, () => { cb() } ) } )
		} )
		async.series( fns, () => {
			Proback.resolver('ok', callback, resolve)
		} )
	} )
}

module.exports = SqsBarrel
