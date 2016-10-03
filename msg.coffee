#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
ws = require 'ws'
mqtt = rqeuire 'mqtt'
uuid = require 'uuid'
Redis = require 'ioredis'
EventEmitter = require 'events'

# A key-value store for all client info.
class ClientChannelRegistry
  constructor: ->
    @clients = {}
    @channels = {}

  getChannels: (clientId) ->
    @clients[clientId] ? {}

  getClients: (channel) ->
    @channels[channel] ? {}

  addClient: (clientId, socket) ->
    if @clients[clientId]?
      @clients[clientId].socket = socket
    else
      @clients[clientId] =
        socket: socket
        channels: {}

  removeClient: (clientId) ->
    emptyChannels = []
    client = @clients[clientId]
    if client?
      if client.channels?
        _(client.channels).keys()
        .each (channel) =>
          if @unsubscribe(clientId, channel) < 1
            emptyChannels.push channel
    delete @clients[clientId]
    emptyChannels

  subscribe: (clientId, channel) ->
    client = @clients[clientId]
    client.channels[channel] = 1
    if not @channels[channel]?
      @channels[channel] = {}
    @channels[channel][clientId] = client.socket
    _(@channels[channel]).size()

  unsubscribe: (clientId, channel) ->
    if @clients[clientId]?
      delete @clients[clientId].channels[channel]
    if @channels[channel]?
      delete @channels[channel][clientId]
      clientCount = _(@channels[channel]).size()
      if clientCount < 1
        delete @channels[channel]
      clientCount
    else
      0

plur = (stem, count) ->
  if count == 1 then "#{count} #{stem}" else "#{count} #{stem}s"

# Core based on broker which speaks MQTT.
class MqttCore extends EventEmitter
  constructor: ->
    console.log "Establishing MQTT connection"

    @client = mqtt.connect('mqtt://localhost');

    @client.on 'message', (topic, message) =>
      @emit 'message', topic, message
    
    @client.on 'connect', =>
      console.log "MQTT client connected"
      @emit 'connected'

    @client.on 'reconnect', =>
      console.log "MQTT client reconnected"
      @emit 'reconnected'

    @client.on 'error', (error) =>
      console.error "MQTT client error:", error
      @emit 'error', error

    @client.on 'close', =>
      console.log "MQTT connection closed"
      @emit 'close'

  subscribe: (channel) ->
    console.log "subscribing to channel '#{channel}'"
    @client.subscribe channel, (error, granted) ->
      if error?
        console.error "Error subscribing to channel '#{channel}':", error
      else
        count = _(granted).size()
        console.log "#{plur 'subscription', count} for this client"

  unsubscribe: (channel) ->
    console.log "un-subscribing from channel '#{channel}'"
    @client.unsubscribe channel, (error) ->
      if error?
        console.error "Error unsubscribing from channel '#{channel}':", error
      else
        console.log "Successfully unsubscribed client from channel #{channel}"

  publish: (channel, message) ->
    @client.publish channel, message

  close: ->
    console.log "Closing MqttCore"
    @client.end()

# Core based on Redis, designed for a distributed cluster of msg servers.
class RedisCore extends EventEmitter
  constructor: ->
    console.log "Establishing Redis connections"

    @sub = new Redis({port: 6379, host: 'localhost'})
    @pub = new Redis({port: 6379, host: 'localhost'})

    @sub.on 'message', (channel, message) =>
      @emit 'message', channel, message

    setImmediate => @emit 'connected'

  subscribe: (channel) ->
    console.log "subscribing to channel '#{channel}'"
    @sub.subscribe channel, (error, count) ->
      if error?
        console.error "Error subscribing to channel '#{channel}':", error
      else
        console.log "#{plur 'subscription', count} in Redis"

  unsubscribe: (channel) ->
    console.log "un-subscribing from channel '#{channel}'"
    @sub.unsubscribe channel, (error, count) ->
      if error?
        console.error "Error unsubscribing from channel '#{channel}':", error
      else
        console.log "#{plur 'subscription', count} in Redis"

  publish: (channel, message) ->
    @pub.publish channel, message

  close: ->
    console.log "Closing RedisCore"
    @sub.close()
    @pub.close()

# Core based on memory, designed for a stand-alone msg server.
class MemoryCore extends EventEmitter
  constructor: ->
    console.log "Nothing to setup in MemoryCore"

    setImmediate => @emit 'connected'

  subscribe: (channel) ->
    console.log "Pretending to subscribe to channel #{channel}"

  unsubscribe: (channel) ->
    console.log "Pretending to unsubscribe from channel #{channel}"

  publish: (channel, message) ->
    setImmediate ->
      @emit 'message', channel, message

  close: ->
    console.log "Closing MemoryCore"

#core = new MqttCore()
core = new RedisCore()
#core = new MemoryCore()
context = new ClientChannelRegistry()
server = new ws.Server({port: 8888})

core.on 'message', (channel, message) ->
  #console.log "Received a message on channel #{channel}"
  sockets = context.getClients(channel)
  #console.log "Forwarding message: #{message} to #{plur 'client', _(sockets).size()}"
  _(sockets)
  .each (socket) ->
    record =
      channel: channel
      message: message
    json = JSON.stringify record
    try
      socket.send json
    catch error
      console.error """Error forwarding message:
        channel: #{channel}
        message: #{message}
        """, error

# Server connection handler
server.on 'connection', (sock) ->
  clientId = uuid.v1()
  console.log "New connection, client #{clientId}"
  context.addClient clientId, sock

  sock.on 'error', (error) ->
    console.error "Connection error: #{error}\n#{error.stack}"

  sock.on 'close', ->
    console.log "Client #{clientId} disconnected."
    _(context.removeClient(clientId))
    .each (channel) ->
      core.unsubscribe channel

  sock.on 'message', (json) ->
    #console.log "Received message: #{json}" 
    try
      record = JSON.parse json
      {action, channel, message} = record

      switch action
        when 'publish'
          if channel? and message?
            core.publish channel, message
        when 'subscribe'
          if channel?
            count = context.subscribe clientId, channel
            console.log "#{plur 'subscriber', count} on channel #{channel}"
            core.subscribe channel
        when 'unsubscribe'
          if channel?
            count = context.unsubscribe clientId, channel
            console.log "#{plur 'subscriber', count} on channel #{channel}"
            if _(context.getClients(channel)).size() < 1
              core.unsubscribe channel
    catch error
      console.error "Invalid JSON: #{error}\n#{error.stack}\n#{json}"

