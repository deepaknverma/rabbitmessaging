/*
 * @Author: Deepak Verma
 * @Date:   2015-09-23 09:30:12
 * @Last Modified by:   dverma
 * @Last Modified time: 2017-04-06 11:58:29
 */

'use strict';
const root = require('../..');
const should = require('should');
const path = require('path');
const fs = require('fs');
let bus, busStatus;

function listen() {
	function reply(msg, done) {
		// console.log(msg);
		if (done) {
			if (msg.payload === 'Success') {
				return done(null, msg);
			} else {
				done(new Error());
			}
		}
	}

	bus.listen({ inqueue: 'd.rabbit.test' }, {
		exchange: 'RABBIT',
		routing: 'direct'
	}, reply)
		.catch(function(err) {
			// console.log(err);
		});

}

function publish() {
	bus.publish('testSubscribeFunction', {
		exchange: 'RABBIT',
		routing: 'direct',
		routingKey: 'desi'
	});
}


describe('Message Bus', function() {

	before(function(done) {
		const env = JSON.parse(fs.readFileSync(path.join(__dirname, '../../env.json'), 'UTF-8'));
		// console.log(env);
		bus = new root.rabbit({
			url: 'amqp://' + env.username + ':' + env.password + '@' + env.host + ':' + env.port,
			prefetch: env.prefetch,
			debug: env.debug,
			xMessageTtl: env.xMessageTtl,
			xExpires: env.xExpires,
			XMaxPriority: env.XMaxPriority
		});
		bus.__connection()
			.then(function(status) {
				busStatus = status;
				listen();
				console.log('Bus connection status: ', status);
				done();
			})
			.catch(function(err) {
				throw err;
			});
	});

	it('should create a rabbit connection', function(done) {
		this.timeout(3000);
		bus.conn.should.have.property('connection');
		should.equal(busStatus, true, 'True');
		done();
	});

	it('should have channel', function(done) {
		this.timeout(3000);
		bus.ch.should.have.property('ch').which.is.a.Number();
		should.equal(bus.ch.ch, 1, 'one');
		done();
	});

	it('should make an rpc call with expiry and recieve success message', function(done) {
		this.timeout(3000);
		bus.rpc({ inqueue: 'd.rabbit.test' }, 'Success', {
			exchange: 'RABBIT',
			routing: 'direct',
			expiration: 10
		})
			.then(function(data) {
				// console.log(data);
				should.exist(data);
				data.should.have.property('payload');
				should.equal(data.payload, 'Success', 'Success');
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.not.exist(err);
				done();
			});
	});

	it('should make an rpc call and recieve success message', function(done) {
		this.timeout(6000);
		bus.rpc({ inqueue: 'd.rabbit.test' }, 'Success', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(data) {
				// console.log(data);
				should.exist(data);
				data.should.have.property('payload');
				should.equal(data.payload, 'Success', 'Success');
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.not.exist(err);
				done();
			});
	});

	it('should subscribe msg based on routing key', function(done) {
		this.timeout(3000);

		function execute(message) {
			// console.log(message);
			should.exist(message);
			should.equal(message, 'testSubscribeFunction', 'testSubscribeFunction');
			done();
		};

		bus.subscribe('desi', {
			exchange: 'RABBIT',
			routing: 'direct',
			targetCounter: 4
		}, execute);
		setTimeout(function() {
			publish();
		}, 100);
	});

	it('should make an rpc call using publish to queue method', function(done) {
		this.timeout(6000);
		bus.publishToQueue({ inqueue: 'd.rabbit.test' }, 'Success', 'rpc', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(data) {
				// console.log(data);
				should.exist(data);
				data.should.have.property('payload');
				should.equal(data.payload, 'Success', 'Success');
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.not.exist(err);
				done();
			});
	});

	it('should make a send call using publish to queue method', function(done) {
		this.timeout(3000);
		bus.publishToQueue({ inqueue: 'd.rabbit.test' }, 'Success', 'send', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(data) {
				should.exist(data);
				should.equal(data, 'Success', 'Msg Sent');
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.not.exist(err);
				done();
			});
	});

	it('should make an rpc call and recieve error message', function(done) {
		this.timeout(3000);
		bus.rpc({ inqueue: 'd.rabbit.test' }, 'Error', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(data) {
				should.not.exist(data);
				done();
			})
			.catch(function(err) {
				should.exist(err);
				should.equal(err._error, true, 'True');
				should.equal(err._queue, 'd.rabbit.test', 'd.rabbit.test');
				done();
			});
	});

	it('should make a send call', function(done) {
		this.timeout(3000);
		bus.send({ inqueue: 'd.rabbit.test' }, 'Success', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(data) {
				should.exist(data);
				should.equal(data, 'Success', 'Msg Sent');
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.not.exist(err);
				done();
			});
	});


	it('should make a listen to a queue', function(done) {
		this.timeout(3000);
		// Lets send the message first and then validate
		bus.send({ inqueue: 'd.rabbit.test' }, 'testSendFunction', {
			exchange: 'RABBIT',
			routing: 'direct'
		});
		done();
		bus.listen({ inqueue: 'd.rabbit.test' }, {
			exchange: 'RABBIT',
			routing: 'direct'
		}, reply);

		function reply(msg, respond) {
			msg.should.have.property('payload');
			should.equal(msg.payload, 'testSendFunction', 'Success');
		}
	});

	it('should publish msg based on routing key', function(done) {
		this.timeout(3000);
		bus.publish('completed', {
			exchange: 'RABBIT',
			routing: 'direct',
			routingKey: 'desi'
		})
			.then(function(data) {
				should.exist(data);
				should.equal(data, 'completed', 'completed');
				done();
			})
			.catch(function(err) {
				should.not.exist(err);
				done();
			});
	});

	it('should fail with no bus options', function(done) {
		this.timeout(3000);
		const newbus = new root.rabbit();
		newbus.__connection()
			.then(function(status) {
				// console.log(status);
				should.not.exist(status);
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.exist(err);
				done();
			});
	});

	it('should fail send method with no connection', function(done) {
		this.timeout(3000);
		const newbus = new root.rabbit();
		newbus.send({ inqueue: 'd.rabbit.test' }, 'testSendFunction', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(status) {
				// console.log(status);
				should.not.exist(status);
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.exist(err);
				done();
			});
	});

	it('should fail rpc method with no connection', function(done) {
		this.timeout(3000);
		const newbus = new root.rabbit();
		newbus.rpc({ inqueue: 'd.rabbit.test' }, 'testSendFunction', {
			exchange: 'RABBIT',
			routing: 'direct'
		})
			.then(function(status) {
				// console.log(status);
				should.not.exist(status);
				done();
			})
			.catch(function(err) {
				// console.log(err);
				should.exist(err);
				done();
			});
	});

	it('should fail listen method with no connection', function(done) {
		this.timeout(3000);
		const newbus = new root.rabbit();

		newbus.listen({ inqueue: 'd.rabbit.test' }, {
			exchange: 'RABBIT',
			routing: 'direct'
		}, reply)
			.then(function(data) {
				should.not.exist(data);
				done();
			})
			.catch(function(err) {
				should.exist(err);
				done();
			});

		function reply(msg, respond) {
			msg.should.have.property('payload');
			should.equal(msg.payload, 'testSendFunction', 'Success');
		}
	});

	it('should close rabbit connection', function(done) {
		const conn = bus.closeConnection();
		// console.log(conn);
		setTimeout(function() {
			should.exist(conn);
			done();
		}, 100);
	});

});
