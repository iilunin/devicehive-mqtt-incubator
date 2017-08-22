'use strict'

// persistence should be changed to mongo/redis or something to work in cluster
// https://github.com/mcollina/aedes-persistence-mongodb
const aedes = require('aedes')({persistence: undefined})
const net = require('net')
const websocket = require('websocket-stream')
const pino = require('pino')({level: process.env.LOG_LEVEL || 'info'})

const wsPort = 8080, tcpPort = 1883
let ws, tcpServer

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
  var packet = {
    cmd: `publish`,
    qos: 2,
    topic: `testtopic/1`,
    payload: Buffer.from(`Test message: ${Math.random()}`),
    retain: true,
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
  pino.info(`aedes.authenticate; '${arguments}'`)
  callback(null, true)
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

  // Don't allow publishing messages from clients
  // TODO: Here we should integrated with DH WS
  // callback(null)

  // We will need to deal with retain packages it can be done
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

  setInterval(publish.bind(this), 5000)
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