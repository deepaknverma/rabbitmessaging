/*
 * @Author: dverma
 * @Date:   2016-04-03 23:04:18
 * @Last Modified by:   dverma
 * @Last Modified time: 2017-05-31 17:55:46
 */

'use strict';

const amqp = require('amqplib');
const errs = require('errs');
const clone = require('clone');
const extend = require('extend');
const uuid = require('node-uuid');
const ip = require('ip');

class rabbit {
	/**
	 * [constructor Intialiizing RabbitMQ configuration. Can be modified on the fly]
	 * @param  {[object]} options [a object with RabbitMQ configuration options]
	 * @return {[object]}         [this object]
	 */
	constructor(options) {
		options = options || {};

		// RabbitMQ configuration Options. if not set default will be used
		this.url = options.url || false;									// URL to the server
		this.prefetch = options.prefetch || 10;								// Number of channels allowed in a single connection
		this.heartbeat = options.heartbeat || 10;							// number of sec after which server send back a connection alive heartbeat else throws an error
		this.reconnectTimeout = options.reconnectTimeout || 10000; 			// Timeout before trying to reconnect to the server
		this.debug = options.debug || false;								// debug the output
		this.conn = false;													// Connection object
		this.maxRetry = options.maxRetry || 60;								// number times the rabbit connection retry after failure. that calculate to 10 mins of retry before service fails						 																		// Number of retry connection count
		this.xMessageTtl = options.xMessageTtl || 60000;					// Time to live for a return queue
		this.xExpires = options.xExpires || 60000;							// Expiry time for for a queue
		this.XMaxPriority = options.XMaxPriority || 10;						// Priority for this queue
		this.noAck = options.noAck || false;								// if true, the broker won't expect an acknowledgement of messages delivered to this consumer
		this.ch = false;													// channel
		this.retry;

		// socket Options for TLS encryption.
		// I Still did not get the time to test it, so use it carefully
		(options.socketOptions)
			? this.caOptions = {
				cert: (options.socketOptions.certificateAsBuffer) ? options.socketOptions.certificateAsBuffer : false,  // client cert
				key: (options.socketOptions.privateKeyAsBuffer) ? options.socketOptions.privateKeyAsBuffer : false,     // client key
				passphrase: (options.socketOptions.passphrase) ? options.socketOptions.passphrase : false, 				// passphrase for key
				ca: (options.socketOptions.caCertAsBuffer) ? [options.socketOptions.caCertAsBuffer] : []				// array of trusted CA certs
			} : undefined;

		this.clientProperties = { IP: ip.address() };
		this.socketOptions = extend({ clientProperties: this.clientProperties }, this.caOptions);

		return this;
	}

	/**
	 * [_bufferify this will create new buffer for the input payload so it can be pushed to queue]
	 * @param  {[object]} message [message payload]
	 * @return {[string]}         [new buffer]
	 */
	_bufferify(message) {
		return new Buffer(JSON.stringify(message));
	};

	/**
	 * [_unbufferify this convert buffert back to an object]
	 * @param  {[string]} message [buffer]
	 * @return {[object]}         [message object]
	 */
	_unbufferify(message) {
		return JSON.parse(new Buffer(message, 'utf8'));
	}

	/**
	 * [cid We need to check]
	 * @param  {[type]} oldProperties [description]
	 * @param  {[type]} newProperties [description]
	 * @return {[type]}               [description]
	 */
	cid(oldProperties, newProperties) {
		newProperties = newProperties || {};

		if (oldProperties && oldProperties.correlationId) {
			newProperties.correlationId = oldProperties.correlationId;
		}

		if (!newProperties.correlationId) {
			newProperties.correlationId = uuid.v4();
		}

		return newProperties;
	}

	/**
	 * [closeConnection description]
	 * @return {[type]} [description]
	 */
	closeConnection() {
		const self = this;
		if (self.retry === self.maxRetry) {
			self.retry = 0;
		}

		try {
			console.log('closeConnection() called - closing connection');
			return self.conn.close();
		}
		catch (err) {
			console.error('close conn err: ', err.message);
		}

	}

	/**
	 * [__constructError create structured error]
	 * @param  {[type]} err     [description]
	 * @param  {[type]} options [description]
	 * @return {[type]}         [description]
	 */
	__constructError(err, options) {
		err = errs.merge(new Error(options.message || 'Unknown Error'), err || {});
		extend(err.stacktrace, errs.create(err.description || err.message).stack.split('\n'));
		return errs.merge(err, {
			title: options.faultstring || 'rabbitMessagingError',
			message: options.message || 'An unexpected error happen',
			parameters: options.parameters || false
		});
	}

	/**
	 * [createChannel Create new channel using the connection established before]
	 * @return {[object]} [return new object if channel is created else error object]
	 */
	createChannel() {
		const self = this;
		return new Promise((success, fail) => {
			self.conn.createChannel()
				.then((ch) => {
					self.ch = ch;
					self.ch.prefetch(self.prefetch);
					self.ch.on('error', (err) => {
						err = err || 'error event emmitted - triggering close';
						console.error('Oops channel Errored. Please check the Error:', err, ' :118');
					});
					self.ch.on('close', (err) => {
						err = err || 'close event triggered';
						console.info('Oops channel Closed. Please check the Error: ', err);
					});
					return success(true);
				})
				.catch((err) => {
					// If debug true then log it
					console.error('Error creating channel. Reason: ', err);
					return fail(self.__constructError(false, { message: err.description || 'Unable to create channel', parameters: self }));
				});
		});
	}

	/**
	 * [__connection this will make new connection to rabbit messaging queue]
	 * @return {[Boolean]} [true if connection is established else error object]
	 */
	__connection() {
		const self = this;
		return new Promise((success, fail) => {
			amqp.connect(self.url + '?heartbeat=' + self.heartbeat, self.socketOptions)
				.then((conn) => {
					console.log('Connected now to: ' + self.url + ' from: ' + ip.address());
					// set the connection object in the instance
					self.conn = conn;
					self.conn.status = false;
					// if an error happen then trigger reconnection
					self.conn.on('error', (err) => {
						console.error('Rabbit error occured', err);
					});

					// if a connection block happen then trigger reconnection
					self.conn.on('blocked', (reason) => {
						console.error('Rabbit blocked connection. Reason: ', reason);
						self.conn.status = true;
						self.__reconnect(self);
					});

					// if connection is closed for some reason then trigger reconnection
					// NOTE: if an error occurs the close event is immediately triggered
					self.conn.on('close', (err) => {
						err = err || 'close event triggered';
						console.info('Rabbit closed connection. Reason: ', err);
						// only of close connection os caused by a fatal error then initiate a new connection else leave it
						self.conn.status = false;
						setTimeout(self.__reconnect(self), 500);
					});
					// Create channel
					return self.createChannel();
				})
				.then((status) => {
					return success(status);
				})
				.catch((err) => {
					console.error('Error creating connection. Reason: ', err);
					return fail(self.__constructError(false, { message: err.description || 'Unable to create connection', parameters: self }));
				});
		});
	}

	/**
	 * [__preserverParameters description]
	 * @param  {[type]} method [description]
	 * @param  {[type]} a      [description]
	 * @param  {[type]} b      [description]
	 * @param  {[type]} c      [description]
	 * @return {[type]}        [description]
	 */
	__preserverParameters(method, a, b, c) {
		const self = this;
		self.method = method;
		self.a = a;
		self.b = b;
		self.c = c;
	}

	/**
	 * [__reconnect This will try to reconnect to rabbit server]
	 * @return {[object]} [connection object set in the instance of this object]
	 */
	__reconnect() {
		const self = this;
		// Lets increment the count first
		self.retry = self.retry + 1;
		// close any previous connection gracefully
		try {
			if (self.conn.status) {
				console.log('closing open connection');
				self.closeConnection();
			}
		}
		catch (err) {
			console.log('Error - no open connection to close: ', err.stackAtStateChange);
		}
		// If debug true then log it
		console.log('Scheduling reconnection in ' + (self.reconnectTimeout / 1000) + 's');
		// Giving sometime for rabbit to settle down
		setTimeout(() => {
			// If bebug true then log it
			console.log('now attempting to reconnect ...');
			// establish connection again
			self.__connection()
				.then(() => {
					// ok we have a successful connection,
					// lets set the retry back to 0
					self.retry = 0;
					// if this was a listen method only then we want to trigger the method
					if (self.method === 'listen' || self.method === 'subscribe') {
						return self[self.method](self.a, self.b, self.c);
					}
					return true;
					// clear the preserved
				})
				.catch((err) => {
					console.error('Fail to reconnect...', err);
					if (self.retry === self.maxRetry) {
						// Exhausted all our options so throwing now
						throw new Error(err);
					}
					// Try to reconnect now
					self.__reconnect(self);
				});
		}, self.reconnectTimeout);
	}


	/**
	 * [send this will send message to specific queue]
	 * @param  {[object]} queues  [name of the queus to poll]
	 * @param  {[object]} msg     [msg object to send ]
	 * @param  {[object]} options [queue related options]
	 * @return {[object]}         [successful send or fail object]
	 */
	send(queues, msg, options) {
		const self = this;
		return new Promise((success, fail) => {
			let ok;
			options = extend(options, self.cid({ durable: false, autoDelete: false, persistent: true, mandatory: true }));

			// we need to check if the connection is not established then return back
			if (!self.conn) {
				return fail(errs.create({
					title: 'rabbitMessagingError',
					message: 'Unable to connect',
					parameters: msg
				}));
			}
			// we need to check if the connection is not established then go ahead and create one
			if (!self.ch) {
				self.createChannel();
			}
			// Check for Exchange and if not created then go ahead and create it
			ok = self.ch.assertExchange((options.exchange) ? options.exchange : 'RABBIT', (options.routing) ? options.routing : 'direct', options);
			// Check for the queue and if not created then go ahead and create it
			ok = self.ch.assertQueue(queues.inqueue, options);

			ok.then(() => {
				self.ch.sendToQueue(queues.inqueue, self._bufferify(msg), options);
				console.log('Msg sent to queue:', queues.inqueue);
				return success(msg);
			})
				.catch((err) => {
					console.log('\n-------------------SEND ERROR----------------------\n' + err + '\n-------------------SEND ERROR CLOSED----------------------\n');
					// Try to reconnect now
					self.__reconnect(self);
					return fail(self.__constructError(false, { message: err.description || 'Unable to send', parameters: self }));
				});
		});
	}

	/**
	 * [rpc This will make a RPC call to the queue and wait for the response through the return queue]
	 * @param  {[object]} queues  [name of the queus to poll]
	 * @param  {[object]} msg     [msg object to send ]
	 * @param  {[object]} options [queue related options]
	 * @return {[object]}         [successful response or fail object]
	 */
	rpc(queues, msg, options) {
		const self = this;
		return new Promise((success, fail) => {
			const corrId = uuid.v4();
			const replyTo = 'return.' + queues.inqueue + '.rpc.' + uuid.v4();
			const timeoutChannel = (options.expiration) ? options.expiration : 1000;
			let ok,
				returnErr = false,
				timeout;

			options = extend(options, self.cid(options));

			if (!options.headers) {
				options.headers = { rejected: 0 };
			}
			options.replyTo = replyTo;
			options.noAck = self.noAck; 	// if true, the broker won't expect an acknowledgement of messages delivered to this consumer

			// we need to check if the connection is not established then return back
			if (!self.conn) {
				return fail(self.__constructError(false, { message: 'Connection not set. Create connection first', parameters: msg }));				
			}
			// we need to check if the connection is not established then go ahead and create one
			if (!self.ch) {
				self.createChannel();
			}
			// check for Queue and if not present then create it
			ok = self.ch.assertQueue(options.replyTo, {
				exclusive: options.exclusive || false, 		// if true, the broker won't let anyone else consume from this queue
				durable: options.durable || false, 			// if true, the queue will survive broker restarts
				autoDelete: options.autoDelete || true, 	// if true, the queue will be deleted when the number of consumers drops to zero (defaults to false)
				arguments: {
					'x-messageTtl': self.xMessageTtl,
					'x-expires': self.xExpires,
					'x-maxPriority': self.XMaxPriority
				}
			})
				.then(() => {
					/**
					 * [ok  Assert a queue into existence. If the queue already exists but has different properties
					 * 		(values supplied in the arguments field may or may not count for borking purposes)]
					 * @type {[type]}
					 * @return {object} [{ queue:'foobar', messageCount: 0, consumerCount: 0 }]
					 */
					return self.ch.assertQueue(queues.inqueue, options);
				})
				.then((thisObj) => {
					/**
					 * { queue: 'd.rabbit.test', messageCount: 0, consumerCount: 1 }
					 */
					// Lets send to queue now
					self.ch.sendToQueue(thisObj.queue, self._bufferify(msg), { correlationId: corrId, replyTo: replyTo });
					// incoming message handler
					const maybeAnswer = (obj) => {
						if (obj.properties.correlationId === corrId) {
							// Check if broker defined an expiration time for this queue
							timeout = setTimeout(() => {
								// remove from queue
								if (obj.fields.consumerTag) {
									self.ch.cancel(obj.fields.consumerTag)
										.then(() => {
											console.log('Removed message from queue after expiration: ' + timeoutChannel + '. with consumerTag:', obj.fields.consumerTag);
										})
										.catch((err) => {
											console.error('Unable to cancel channel with consumerTag: ', obj.fields.consumerTag, ' with error ', err);
										});
								}
							}, timeoutChannel);
							let returnMessage = self._unbufferify(obj.content);
							// Support for returning errors
							if (returnMessage._error) {
								returnErr = errs.create(returnMessage);
								returnMessage = null;
								return fail(returnErr);
							}
							return success(returnMessage);
						}
					};
					ok = ok.then((queue) => {
						clearTimeout(timeout);
						options.priority = self.XMaxPriority; 							// gives a priority to the consumer; higher priority consumers get messages in preference to lower priority consumers.
						return self.ch.consume(options.replyTo, maybeAnswer, options)
							.then(() => {
								return queue;
							});
					});
				})
				.catch((err) => {
					console.log('\n-------------------RPC ERROR----------------------\n' + err + '\n-------------------RPC ERROR CLOSED----------------------\n');
					// Try to reconnect now
					self.__reconnect(self);
					return fail(self.__constructError(false, { message: err.description || 'Unable to make RPC call', parameters: self }));					
				});
		});
	}

	/**
	 * [listen this will listen to defined queue]
	 * @param  {[object]} queues  [name of the queus to poll]
	 * @param  {[object]} msg     [msg object to send ]
	 * @param  {[function]} confirm [description]
	 * @return {[object]}         [fail object or confirmation]
	 */
	listen(queues, options, confirm) {
		const self = this;
		return new Promise((success, fail) => {
			let ok;
			// preserver parameter
			self.__preserverParameters('listen', queues, options, confirm);

			options = extend(options, self.cid({
				durable: options.durable || true,
				autoDelete: options.autoDelete || false,
				noAck: options.noAck || self.noAck,
				allUpTo: options.allUpTo || true
			}));

			if (!self.conn) {
				return fail(self.__constructError(false, { message: 'Connection not set. Create connection first', parameters: self }));		
			}
			// we need to check if the connection is not established then go ahead and create one
			if (!self.ch) {
				self.createChannel();
			}
			// Check for Exchange and if not created then go ahead and create it
			ok = self.ch.assertExchange((options.exchange) ? options.exchange : 'RABBIT', (options.routing) ? options.routing : 'direct', options);
			// Check for the queue and if not created then go ahead and create it
			ok = self.ch.assertQueue(queues.inqueue, options);

			ok = ok.then(() => {
				// pass consumer tag
				options = self.cid({ consumerTag: ok.consumerTag });
				// process passed to the consume channel to retrieve the message sent on a queue
				const thisPayload = (msg) => {
					const obj = msg;
					// Process to reply back to rabbit with replyto message or just the acknowledgement
					const reply = (returnError, returnMessage) => {
						let result = {};
						// thisSecs = obj.content.toString().split('.').length - 1,
						// retry - determine what to return
						if (returnError) {
							if (obj.properties.headers.rejected < (obj.properties.headers.retries || 0)) {
								console.error('message bus - listen error', returnError);
								obj.properties.headers.rejected++;
								// retry by rejecting the original message and sending a new one
								// with an updated rejected header count
								self.ch.reject(obj, false);
								return self.ch.sendToQueue(obj.properties.replyTo, obj.content, obj.properties);
							}
							result = {
								message: returnError.message,
								stack: returnError.stack,
								code: returnError.code || null,
								description: returnError.description || null,
								detail: returnError.detail || null,
								_error: true,
								_queue: obj.fields.routingKey,
								_options: obj.properties,
								_inputMessage: clone(obj.content.toString()),
								_outputMessage: clone(returnMessage || null),
								_datetime: new Date()
							};
							console.error('Error occured. Pushed back', returnError);

							if (self.errorQueue) {
								self.ch.sendToQueue(self.errorQueue, self._bufferify(result), self.cid(obj.properties));
							}
						} else {
							result = returnMessage || {};
						}
						// reply if required
						if (obj.properties.replyTo) {
							self.ch.sendToQueue(obj.properties.replyTo, self._bufferify(result), options = self.cid({ correlationId: obj.properties.correlationId }));
						}

						try {
							console.log(' [x] Done');
							self.ch.ack(obj);
						}
						catch (e) {
							console.error('Oops! Error acknowledging. rejecting the object', e, result);
							// self.ch.reject(obj, false);
						}
					};
					confirm.call(self, { payload: self._unbufferify(obj.content) }, reply);
				};
				return self.ch.consume(queues.inqueue, thisPayload, options);
			})
				.then(() => {
					console.log('Listening to queue...');
				})
				.catch((err) => {
					console.error('Oops! Error occurred listening to queue...', err);
					console.log('\n-------------------Listen ERROR----------------------\n' + err + '\n-------------------Listen ERROR CLOSED----------------------\n');
					// Try to reconnect now
					self.__reconnect(self);
					return fail(self.__constructError(false, { message: err.description || 'Unable to listen', parameters: self }));		
				});
		});
	}

	/**
	 * [subscribe This process will subscribe to a channel based on the topic]
	 * @param  {[object]} queues  [routing key to poll in the exchange]
	 * @param  {[object]} options     [poll relation options]
	 * @param  {[function]} confirm [description]
	 * @return {[object]}         [fail object or confirmation]
	 */
	subscribe(topic, options, confirm) {
		const self = this;
		return new Promise((success, fail) => {
			const queue = topic + '.sub.' + uuid.v4();
			const timeoutChannel = (options.expiration) ? options.expiration : 8000;
			let obj,
				ok;

			// preserver parameter
			self.__preserverParameters('subscribe', topic, options, confirm);

			if (typeof options === 'function') {
				options = self.cid();
			}
			if (!self.conn) {
				return fail(self.__constructError(false, { message: 'Connection not set. Create connection first', parameters: self }));
			}
			// we need to check if the connection is not established then go ahead and create one
			if (!self.ch) {
				self.createChannel();
			}
			// Handler for incoming message
			const logMessage = (msg) => {
				obj = msg;
				confirm.call(self, self._unbufferify(obj.content));
				// wait to timeout the queue
				setTimeout(() => {
					// remove from queue
					self.ch.cancel(obj.fields.consumerTag)
						.then(() => {
							console.log('Removed message from queue after expiration: ' + timeoutChannel + '. with consumerTag:', obj.fields.consumerTag);
						})
						.catch((err) => {
							console.error('Unable to cancel channel with consumerTag: ', obj.fields.consumerTag, ' with error ', err);
						});
				}, timeoutChannel);
			};
			// Check for Exchange and if not created then go ahead and create it
			ok = self.ch.assertExchange((options.exchange) ? options.exchange : 'RABBIT', (options.routing) ? options.routing : 'fanout', options);

			// Check for the queue and if not created then go ahead and create it
			ok = ok.then(() => {
				return self.ch.assertQueue(queue, { 'exclusive': true, 'durable': false, 'autoDelete': true, 'x-expires': (options.expiration) ? options.expiration : 1000 * 8 });
			});

			ok = ok.then(() => {
				// console.log(qok);
				return self.ch.bindQueue(queue, (options.exchange) ? options.exchange : 'RABBIT', topic, options)
					.then(() => {
						return queue;
					});
			});

			ok = ok.then((queue) => {
				return self.ch.consume(queue, logMessage, { noAck: self.noAck });
			});

			return ok.then(() => {
				console.log('Listening to queue...');
			});
		});
	}

	/**
	 * [publish Publish a single message to an exchange]
	 * @param  {[type]} msg [description]
	 * @return {[type]}     [description]
	 */
	publish(msg, options) {
		const self = this;
		return new Promise((success, fail) => {
			let ok;
			if (typeof options === 'function') {
				options = self.cid({ durable: false, autoDelete: false, noAck: self.noAck });
			}

			if (!self.conn) {
				return fail(self.__constructError(false, { message: 'Connection not set. Create connection first', parameters: self }));
			}
			// we need to check if the connection is not established then go ahead and create one
			if (!self.ch) {
				self.createChannel();
			}
			ok = self.ch.assertExchange((options.exchange) ? options.exchange : 'RABBIT', (options.routing) ? options.routing : 'fanout', options);
			ok.then(() => {
				self.ch.publish((options.exchange) ? options.exchange : 'RABBIT', (options.routingKey) ? options.routingKey : 'fanout', self._bufferify(msg), options);
				console.log('Msg published to exchange with routingKey:', options.routingKey);
				return success(msg);
			});
		});
	}

	/**
	 * [publishToQueue push the payload to the update title queue]
	 * @param  {[object]} msg [payload to be recorded into the DB propertyId, titleData]
	 * @return {[object]}     [error or pushed payload]
	 */
	publishToQueue(queues, message, method, options) {
		const self = this;
		return new Promise((success, fail) => {
			queues = queues || false;
			method = method || 'rpc';
			// check if queues are provided to push to queue
			if (!queues) {
				return fail(self.__constructError(false, { message: 'Unable to publish to queue. No queue provided.', parameters: self }));				
			}

			// check if message are provided to push to queue
			if (!message) {
				return fail(self.__constructError(false, { message: 'Unable to publish to queue. No message provided.', parameters: self }));				

			}
			// lets push to queue
			self[method](queues, message, {
				exchange: options.exchange || 'amq.direct',
				routing: options.routing || 'direct'
			})
				.then((data) => {
					console.log(data);
					return success(data);
				})
				.catch((err) => {
					console.error(err);
					return fail(errs.merge(err, errs.create({
						title: 'rabbitMessagingError',
						message: err.description,
						parameters: self
					})));
				});
		});
	}


	// TODO: need to fix this
	// /**
	//  * [broadcast description]
	//  * @param  {[type]} msg [description]
	//  * @return {[type]}     [description]
	//  */
	// broadcast(queues, msg, options) {
	// 	let self = this,
	// 		ok;

	// 	if (typeof options === 'function') {
	// 		options = self.cid({ durable: true, autoDelete: false, noAck: false, deliveryMode: true });
	// 	}

	// return new Promise(function(success, fail) {
	// 	if (!self.conn) {
	// 		self.error.message = 'Unable to connect';
	// 		self.error.parameters = msg;
	// 		return fail(self.error);
	// 	}
	// 		self.conn()
	// 		.then(function(status) {
	// 			return self.createChannel();
	// 		})
	// 		.then(function(ch) {
	// 			ok = ch.assertQueue(queues.inqueue, options);
	// 			ok.then(function(_qok) {
	// 				ch.sendToQueue(queues.inqueue, self._bufferify(msg), options);
	// 				// console.log(" [x] Sent '%s'", msg);
	// 				ch.close();
	// 			});
	// 		})
	// 		.catch(function(err) {
	// 			self.error = errs.merge(self.error, errs);
	// 			return fail(self.error);
	// 		});
	// 	});
	// }
}
module.exports = {
	rabbit: rabbit
};
