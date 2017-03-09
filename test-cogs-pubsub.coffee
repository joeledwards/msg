#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
Q = require 'q'
fs = require 'fs'
uuid = require 'uuid'
cogs = require 'cogs-sdk'
moment = require 'moment'
durations = require 'durations'

{keys} = JSON.parse(fs.readFileSync('cogs-pubsub.json', 'utf-8'))

now = -> moment().valueOf()

initialDelay = 2000
#messageDelay = 1
messageDelay = 0
messageCount = 100
remaining = messageCount + 1

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

# Subscribe to the channel
subscribe = (handle) ->
  handle.subscribe 'messages', ({message: json}) ->
    ts = now()
    {id} = JSON.parse json
    msg = messages[id]
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
    console.log "Message '#{id}' (#{received} of #{sent}) received in #{msg.timing.total} ms (Delivery #{msg.timing.delivery} ms, Cogs P/S #{msg.timing.broker} ms): #{json}"

    if remaining < 1 and received >= sent
      handle.unsubscribe 'messages'
      .then -> summarize()

# recursive message delivery function
sendMsg = (handle) ->
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

      handle.publishWithAck 'messages', json
      .then (result) ->
        sent += 1
        ts = now()
        msg.timing.published = ts
        pubTime = msg.timing.published - msg.timing.sent
        console.log "Message ##{sent} '#{msg.id}' delivered in #{pubTime} ms : #{result}"
        sendMsg handle
      .catch (error) ->
        console.error "Error sending message: #{error}\n#{error.stack}"
        msg.error = true
        sendMsg handle

# connect after 2 sec. to give the subscription some time to "take"
Q.delay initialDelay
.then -> cogs.pubsub.connect keys
.then (handle) ->
  subscribe handle
  .then ->
    watch.start()
    sendMsg handle
.catch (error) ->
  console.error "Error setting up test:", error

