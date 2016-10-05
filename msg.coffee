#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
P = require 'bluebird'
fs = require 'fs'
ws = require 'ws'
mqtt = require 'mqtt'
uuid = require 'uuid'
Redis = require 'ioredis'
EventEmitter = require 'events'

# A key-value store for all client info.
class ClientChannelRegistry
  constructor: ->
    @clients = {}
    @channels = {}

  markChannelSend: (channelKey) ->
    channel = @channels[channelKey]
    if channel?
      channel.send += 1

  markChannelRecv: (channelKey) ->
    channel = @channels[channelKey]
    if channel?
      channel.recv += 1

  markClientSend: (clientId) ->
    client = @clients[clientId]
    if client?
      client.send += 1

  markClientRecv: (clientId) ->
    client = @clients[clientId]
    if client?
      client.recv += 1

  # Fetch all channels associated with a client
  getClientChannels: (clientId) ->
    client = @clients[clientId] ? {}
    client.channels

  # Fetch all clients associated with a channel
  getChannelClients: (channelKey) ->
    channel = @channels[channelKey] ? {}
    channel.clients

  # Add a client
  addClient: (clientId, socket) ->
    client = @clients[clientId]

    if client?
      client.socket = socket
    else
      @clients[clientId] =
        socket: socket
        send: 0
        recv: 0
        channels: {}

  # Remove a client
  removeClient: (clientId) ->
    emptyChannels = []
    client = @clients[clientId]

    if client?
      channels = client.channels

      if channels?
        _(channels)
        .keys()
        .each (channelKey) =>
          if @unsubscribe(clientId, channelKey) < 1
            emptyChannels.push channelKey

      console.log "Removing client #{clientId}; sent=#{client.send} received=#{client.recv}"

    delete @clients[clientId]
    emptyChannels

  # Subscribe a client to a channel
  subscribe: (clientId, channelKey) ->
    client = @clients[clientId]
    client.channels[channelKey] = 1
    channel = @channels[channelKey]

    if not channel?
      channel =
        send: 0
        recv: 0
        clients: {}
      @channels[channelKey] = channel

    channel.clients[clientId] = client.socket

    _(channel.clients).size()

  # Unsubscribe a client from a channel
  unsubscribe: (clientId, channelKey) ->
    client = @clients[clientId]

    if client?
      delete client.channels[channelKey]

    channel = @channels[channelKey]

    if channel?
      delete channel.clients[clientId]
      clientCount = _(channel.clients).size()

      if clientCount < 1
        if channel.send > 0 and channel.recv > 0 and channel.send != channel.recv
          console.log "============================================================================="
          console.log "-----------------------------------------------------------------------------"
          console.log "Removing channel #{channelKey}; sent=#{channel.send} received=#{channel.recv}"
          console.log "-----------------------------------------------------------------------------"
          console.log "============================================================================="
        else
          console.log "Removing channel #{channelKey}; sent=#{channel.send} received=#{channel.recv}"

        delete @channels[channelKey]

      clientCount
    else
      0

plur = (stem, count) ->
  if count == 1 then "#{count} #{stem}" else "#{count} #{stem}s"

class BaseCore extends EventEmitter
  constructor: ->
    @paused = false

  pause: ->
    @paused = true

  resume: ->
    @paused = false

  isPaused: ->
    @paused

# Core based on broker which speaks MQTT.
class MqttCore extends BaseCore
  constructor: (servers) ->
    super()

    console.log "Establishing MQTT connection"

    @client = mqtt.connect({servers: servers})

    @client.on 'message', (topic, message) =>
      if not @paused
        @emit 'message', topic, JSON.parse(message)
    
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
    new P (resolve, reject) => 
      console.log "subscribing to channel '#{channel}'"
      @client.subscribe channel, (error, granted) ->
        if error?
          console.error "Error subscribing to channel '#{channel}':", error
          reject error
        else
          count = _(granted).size()
          console.log "#{plur 'subscription', count} for this client"
          resolve()

  unsubscribe: (channel) ->
    new P (resolve, reject) =>
      console.log "un-subscribing from channel '#{channel}'"
      @client.unsubscribe channel, (error) ->
        if error?
          console.error "Error unsubscribing from channel '#{channel}':", error
          reject error
        else
          console.log "Successfully unsubscribed client from channel #{channel}"
          resolve()

  publish: (channel, message) ->
    @client.publish channel, JSON.stringify(message)

  close: ->
    console.log "Closing MqttCore"
    @client.end()

# Core based on Redis, designed for a distributed cluster of msg servers.
class RedisCore extends BaseCore
  constructor: ({cluster, server, servers}) ->
    super()

    console.log "Establishing Redis connections"

    @sub = if cluster then new Redis.Cluster(servers) else new Redis(server)
    @pub = if cluster then new Redis.Cluster(servers) else new Redis(server)

    @sub.on 'message', (channel, message) =>
      if not @paused
        @emit 'message', channel, message

    setImmediate => @emit 'connected'

  subscribe: (channel) ->
    new P (resolve, reject) =>
      console.log "subscribing to channel '#{channel}'"
      @sub.subscribe channel, (error, count) ->
        if error?
          console.error "Error subscribing to channel '#{channel}':", error
          reject error
        else
          console.log "#{plur 'subscription', count} in Redis"
          resolve()

  unsubscribe: (channel) ->
    new P (resolve, reject) => 
      console.log "un-subscribing from channel '#{channel}'"
      @sub.unsubscribe channel, (error, count) ->
        if error?
          console.error "Error unsubscribing from channel '#{channel}':", error
          reject error
        else
          console.log "#{plur 'subscription', count} in Redis"
          resolve()

  publish: (channel, message) ->
    @pub.publish channel, message

  close: ->
    console.log "Closing RedisCore"
    @sub.close()
    @pub.close()

# Core based on memory, designed for a stand-alone msg server.
class MemoryCore extends BaseCore
  constructor: ->
    super()

    console.log "Nothing to setup in MemoryCore"

    setImmediate => @emit 'connected'

  subscribe: (channel) ->
    console.log "Pretending to subscribe to channel #{channel}"

  unsubscribe: (channel) ->
    console.log "Pretending to unsubscribe from channel #{channel}"
    P.resolve()

  publish: (channel, message) ->
    if not @paused
      setImmediate =>
        @emit 'message', channel, message

  close: ->
    console.log "Closing MemoryCore"

config = JSON.parse(fs.readFileSync('config.json', 'utf-8'))

core = new RedisCore(config.redis)
#core = new MqttCore(config.mqtt.servers)
#core = new MemoryCore()
context = new ClientChannelRegistry()
server = new ws.Server({port: 8888})

core.on 'message', (channel, message) ->
  context.markChannelRecv channel
  
  _(context.getChannelClients(channel))
  .toPairs()
  .each ([clientId, socket]) ->
    record =
      action: 'message'
      channel: channel
      message: message

    json = JSON.stringify record

    try
      socket.send json
      context.markClientSend clientId
    catch error
      console.error """Error forwarding message:
        channel: #{channel}
        message: #{JSON.stringify(message)}
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
    try
      record = JSON.parse json
      {action, channel, message} = record

      switch action
        when 'publish'
          if channel? and message?
            context.markClientRecv clientId
            core.publish channel, message
            context.markChannelSend channel
          else
            logger.error "Publish record missing channel or message!"

        when 'subscribe'
          if channel?
            core.subscribe channel
            .then ->
              count = context.subscribe clientId, channel
              console.log "#{plur 'subscriber', count} on channel #{channel}"
              sock.send JSON.stringify({
                action: 'subscribe'
                channel: channel
                result: 'success'
              })
            .catch (error) ->
              console.error "Error subscribing client #{clientId} to channel #{channel}:", error
              sock.send JSON.stringify({
                action: 'subscribe'
                channel: channel
                result: 'failure'
              })
          else
            logger.error "Subscribe record missing channel!"

        when 'unsubscribe'
          if channel?
            unsub = P.resolve()

            if _(context.getChannelClients(channel)).size() < 1
              unsub = core.unsubscribe channel

            unsub
            .then ->
              count = context.unsubscribe clientId, channel
              console.log "#{plur 'subscriber', count} on channel #{channel}"
              sock.send JSON.stringify({
                action: 'unsubscribe'
                channel: channel
                result: 'success'
              })
            .catch (error) -> 
              console.error "Error unsubscribing client #{clientId} from channel #{channel}:", error
              sock.send JSON.stringify({
                action: 'unsubscribe'
                channel: channel
                result: 'failure'
              })
          else
            logger.error "Unsubscribe record missing channel!"
    catch error
      console.error "Invalid JSON: #{error}\n#{error.stack}\n#{json}"

