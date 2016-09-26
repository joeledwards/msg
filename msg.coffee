#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
ws = require 'ws'
uuid = require 'uuid'
Redis = require 'ioredis'

class ClientChannels
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

plur = (count) ->
  if count == 1 then '' else 's'

# Subscribe function
subscribe = (redis, channel) ->
  console.log "subscribing to channel '#{channel}'"
  redis.subscribe channel, (error, count) ->
    if error?
      console.error "Error subscribing to channel '#{channel}': #{error}\n#{error.stack}"
    else
      console.log "#{count} subscription#{plur count}"

# Un-subscribe function
unsubscribe = (redis, channel) ->
  console.log "un-subscribing from channel '#{channel}'"
  redis.unsubscribe channel, (error, count) ->
    if error?
      console.error "Error unsubscribing from channel '#{channel}: #{error}\n#{error.stack}"
    else
      console.log "#{count} subscription#{plur count}"

sub = new Redis({port: 6379, host: 'localhost'})
pub = new Redis({port: 6379, host: 'localhost'})
context = new ClientChannels()
server = new ws.Server({port: 8888})

sub.on 'message', (channel, message) ->
  sockets = context.getClients(channel)
  console.log "Forwarding message: #{message} to #{_(sockets).size(0)} clients"
  _(sockets)
  .each (socket) ->
    record =
      channel: channel
      message: message
    json = JSON.stringify record
    socket.send json

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
      unsubscribe sub, channel

  sock.on 'message', (json) ->
    console.log "Received message: #{json}"
    try
      record = JSON.parse json
      {action, channel, message} = record

      switch action
        when 'publish'
          if channel? and message?
            pub.publish channel, message
        when 'subscribe'
          if channel?
            context.subscribe clientId, channel
            subscribe sub, channel
        when 'unsubscribe'
          if channel?
            context.unsubscribe clientId, channel
            if _(context.getClients(channel)).size() < 1
              unsubscribe sub, channel
    catch error
      console.error "Invalid JSON: #{error}\n#{error.stack}\n#{json}"

