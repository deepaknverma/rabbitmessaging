# rabbitmessaging 
[![Build Status](https://travis-ci.org/deepaknverma/rabbitmessaging.svg?branch=master)](https://travis-ci.org/deepaknverma/rabbitmessaging)

Node promise library to handle complex RabbitMQ actions.

## Overview:
___

Microservices are small programs to handle single task form a complete process chain. The right pattern of communication between microservices is the key to scale your application and solve most of the distributed system problems.
RabbitMQ provide a event driven approach to send messages to each other in a Microservice architecture.

### For More info of RabbitMQ see:
https://www.rabbitmq.com/
http://howtocookmicroservices.com/communication/
http://blog.runnable.com/post/150022242931/event-driven-microservices-using-rabbitmq

This library provide a simple wrapper on amqp to interact with Rabbit using following approach:
	
- Broker
- Publisher
- Subscriber

## Features:
___

- Automatically try to reconnect 60 times (configurable) at 1 second apart if broker dies
- Round-robin connection between multiple brokers in a cluster
- Handles messages in memory
- TLS encryption support
- Provide option to `rpc`, `send`, `listen`, `subscribe` and `publish`

## __connection: 
___
Create a new amqp connection  which will connect to provided url in following structure: 

```javascript
	url: 'amqp://' + rabbitmq.username + ':' + rabbitmq.password + '@' + rabbitmq.host + ':' + rabbitmq.port
```
**Options:**

- `url` - URL to the server `Default: false`
- `prefetch` - Number of channels allowed in a single connection `Default: 10`
- `heartbeat` - number of sec after which server send back a connection alive heartbeat else throws an error `Default: 10`
- `reconnectTimeout` - Timeout before trying to reconnect to the server `Default: 10000`
- `debug` - debug the output `Default: false`
- `conn` - Connection object `Default: false`
- `maxRetry` - number times the rabbit connection retry after failure. that calculate to 10 mins of retry before service fails `Default: 60`
- `xMessageTtl` - Time to live for a return queue `Default: 60000`
- `xExpires` - Expiry time for for a queue `Default: 60000`
- `XMaxPriority` - Priority for this queue `Default: 10`
- `noAck` - if true, the broker won't expect an acknowledgement of messages delivered to this consumer `Default: false`
- `ch` - channel `Default: false`
- `caOptions` - socket Options for TLS encryption. `default: undefined`
	- `cert` - client cert
	- `key` - client key
	- `passphrase` - passphrase for key
	- `ca` - array of trusted CA certs
	- `clientProperties` -  user defined properties `{ IP: 127.0.0.1 }`

**Examples:**  see [GITHUB](https://github.com/deepaknverma/rabbitmessaging)
