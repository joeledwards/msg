#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
Q = require 'q'
fs = require 'fs'
uuid = require 'uuid'
cogs = require 'cogs-sdk'
moment = require 'moment'
durations = require 'durations'

now = -> moment().valueOf()

initialDelay = 2000
messageDelay = 1
messageCount = 100
remaining = messageCount

counts = {}
messages = {}
sent = 0
received = 0
watch = durations.stopwatch()

# Summarize all of the message deliveries
summarize = ->
  totals = _(messages)
  .map ({id, timing: {total}}) ->
    id: id
    total: total
  .sortBy ({total}) -> total

  longest = totals.last().total
  shortest = totals.first().total
  mean = _.sum(totals.map(({total}) -> total).value()) / totals.size()

  #_(messages).each (message) -> console.log "#{moment(message.timing.created).toISOString()},#{message.timing.total}"

  console.log "messages=#{messageCount} shortest=#{shortest} longest=#{longest} mean=#{mean} : took #{watch}"

cogs.api.getClient 'cogs.json'
.then (client) -> 
  namespace = 'messenger'

  # recursive message delivery function
  sendMsg = (client) ->
    if remaining > 0
      remaining -= 1
      Q.delay messageDelay
      .then ->
        msg =
          id: uuid.v1()
          timing:
            created: now()

        console.log "Sending message #{msg.id}..."
        messages[msg.id] = msg
        counts[msg.id] = 0

        json = JSON.stringify msg
        msg.timing.sent = now()
        eventName = "message-#{msg.id}"
        attributes =
          channel: 'messages'
          message: JSON.stringify(msg)

        client.sendEvent namespace, eventName, attributes
        .then (result) ->
          sent += 1
          ts = now()
          msg.timing.published = ts
          pubTime = msg.timing.published - msg.timing.sent
          console.log "Message ##{sent} '#{msg.id}' delivered in #{pubTime} ms : #{result}"
          sendMsg client
        .catch (error) ->
          console.error "Error sending message: #{error}\n#{error.stack}"
          msg.error = true
          sendMsg client

  channel =
    channel: 'messages'

  # Subscribe to the channel
  ws = client.subscribe namespace, channel

  ws.once 'open', ->
    console.log 'Subscribed to the messages channel.'
    watch.start()
    sendMsg client

  ws.on 'message', (message) ->
    ts = now()
    {data} = JSON.parse message
    {id, timing} = JSON.parse data
    msg = messages[id]

    console.log "data:", data
    console.log "message-id:", id
    console.log "message-timing:", timing
    console.log "msg:", msg

    if not msg.timing?
      msg.timing = {}

    count = counts[id]

    if not count?
      console.log "UNKNOWN ===============================> #{id}"
      counts[id] = 1
    else
      if count > 0
        console.log " DUPLICATE ===========================> #{id}"
      count += 1
      counts[id] = count
    msg.timing.received = ts
    received += 1
    msg.timing.delivery = msg.timing.published - msg.timing.sent
    msg.timing.broker = msg.timing.received - msg.timing.published
    msg.timing.total = msg.timing.received - msg.timing.sent
    console.log "Message '#{id}' (#{received} of #{sent}) received in #{msg.timing.total} ms (Delivery #{msg.timing.delivery}, Cogs #{msg.timing.broker} ms): #{message}"

    if remaining < 1 and received >= sent
      summarize()
      ws.close()

  ws.on 'error', (error) ->
    console.error "Cogs WebSocket error #{error}\n#{error.stack}"

  ws.on 'ack', (messageId) ->
    console.log "Message #{messageId} acknowledged."

  ws.once 'close', ->
    console.log "Cogs WebSocket closed."

