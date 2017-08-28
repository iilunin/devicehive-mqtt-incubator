'use strict'

// persistence should be changed to mongo/redis or something to work in cluster
// https://github.com/mcollina/aedes-persistence-mongodb
const aedes = require('aedes')({persistence: undefined})
const net = require('net')
const websocket = require('websocket-stream')
const pino = require('pino')({level: process.env.LOG_LEVEL || 'info'})

const wsPort = 8080, tcpPort = 1883
let ws, tcpServer
const badUsername = 'nouser', badTopic = 'noaccess'

/**
 * Creates websocket MQTT broker
 */
function mqttOverWS () {
  ws = websocket.createServer(
    {port: wsPort},
    aedes.handle)

  ws.on('listening', function () {
    pino.info(`MQTT WebSocket Broker is listening on port ${wsPort}`)
  })
}

/**
 * Creates TCP MQTT broker
 */
function mqttOverTcp () {
  tcpServer = net.createServer(aedes.handle)
  tcpServer.listen(tcpPort, function () {
    pino.info(`MQTT TCP Broker is listening on port ${tcpPort}`)
  })
}

function launchMqtt () {
  mqttOverTcp()
  mqttOverWS()
}

function publish () {
  let packet = {
    cmd: `publish`,
    qos: 2,
    topic: `testtopic/1`,
    payload: Buffer.from(`Test message: ${Math.random()}`),
    retain: false,
    c: true
  }
  aedes.publish(packet)
}

/**
 * Override authentication routine
 * @param client
 * @param username
 * @param password
 * @param callback (error, authenticated)
 */
aedes.authenticate = function(client, username, password, callback){
  pino.info(`aedes.authenticate; '${client}'`)
  if (username === badUsername) {
    callback(null, false)
    return
  }

  callback(null, true)
  if (!client.clean) {
    // TODO: load all subscriptions from the database to this client
  }
  if (client.will) {
    // TODO: if client has last will
    // client.will.qos
    // client.will.retain
    // client.will.topic
    // client.will.payload
  }
}

/**
 *
 * Authorizes client to publish message to the broker
 *
 * @param client
 * @param packet
 * @param callback - if callback is executed,
 * then message is sent to the broker's topic
 */
aedes.authorizePublish = function (client, packet, callback) {

  pino.info(`aedes.authorizePublish; ${client.id}: ${getPacketData(packet)}`)

  if (packet.topic.startsWith(badTopic)) {
    client.close()
    return
  }


  // If callback comment, it doesn't publish messages from the clients
  // TODO: Here we should integrated with DH WS
  callback(null)

  // TODO: We will need to deal with retain packages it can be done
  // using MQTT persistence layer or using DH persistence
  if (packet.retain) {
    aedes.persistence.storeRetained(packet, function cb(p) {})
  }
}

/**
 *
 * Authorizes client to subscribe to topic
 *
 * @param client
 * @param sub
 * @param callback
 */
aedes.authorizeSubscribe = function (client, sub, callback) {
  pino.info(`aedes.authorizeSubscribe; ${client.id}: '${JSON.stringify(sub)}'`)

  if (sub.topic.startsWith(badTopic)) {
    client.close()
    return
  }

  callback(null, sub)
}

/**
 *
 * Authorizes forward messages to subscribed clients
 *
 * @param client
 * @param packet
 * @returns {*}
 */
aedes.authorizeForward = function (client, packet) {
  pino.info(`aedes.authorizeForward; ${client.id}: ${getPacketData(packet)}`)
  return packet
}

aedes.published = function (packet, client, callback) {
  pino.info(`aedes.published; ${client ? client.id : '-'}: ${getPacketData(packet)}`)
  callback(null)
}

function getPacketData(p){
  return `packet = {cmd:${p.cmd}; topic: ${p.topic}; qos: ${p.qos}, retain: ${p.retain}}, payload: ${p.payload.toString()}`
}

aedes.on('clientError', function (client, err) {
  pino.info(`client error '${client.id}'; msg: ${err.message}; stack: ${err.stack}`)
})

aedes.on('publish', function (packet, client) {
  if (client) {
    pino.info(`message from client ${client ? client.id : '-'}: ${getPacketData(packet)}`)
  }
})

// We can subscribe client to hist topics here
aedes.on('client', function (client) {
  pino.info(`new client '${client.id}' connected`)

  // setInterval(publish.bind(this), 5000)
})

aedes.on('clientDisconnect', function (client) {
  pino.info(`client '${client.id}' disconnected`)
})

aedes.on('subscribe', function (topic, client) {
  pino.info(`client '${client.id}' subscribed to topic ${JSON.stringify(topic)}`)
})

aedes.on('unsubscribe', function (topic, client) {
  pino.info(`client '${client.id}' unsubscribed from topic ${JSON.stringify(topic)}`)
})

launchMqtt()