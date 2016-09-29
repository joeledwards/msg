#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
P = require 'bluebird'
uuid = require 'uuid'
pubnub = require 'pubnub'
moment = require 'moment'
cluster = require 'cluster'
durations = require 'durations'
WebSocket = require 'ws'

uri = 'ws://localhost:8888'
runDuration = 5000
channelCount = 10
pubWorkers = 2
subWorkers = 2
subscriberCount = 4
publishDelay = 100

channels = [1..channelCount].map -> uuid.v4()
now = -> moment().valueOf()

#uri = 'ws://52.39.3.158:8888'

# Run an individual publisher
runPubWorker = ({id, channelGroup}) ->
  return new P (resolve, reject) ->
    webSockets = []
    active = 0
    published = 0
    summary = {}

    watch = durations.stopwatch().start()
    lastSummaryWatch = durations.stopwatch().start()

    # Summarize all of the message deliveries
    summarize = ->
      console.log "[#{process.pid}] Test ran for #{watch}
        \n  published=#{published}"

    timedSummary = ->
      if lastSummaryWatch.duration().millis() >= 1000
        summarize()
        lastSummaryWatch.reset().start()

    finalSummary = -> summarize() if active < 1

    channelGroup.forEach (channel) ->
      closed = false
      ws = new WebSocket(uri)

      ws.once 'open', ->
        active += 1
        console.log "[#{process.pid}] Publisher #{id} to channel '#{channel}' connected"
        watch.start()
        setTimeout closeWebSockets, runDuration

        sendMessage = () ->
          try
            if not closed
              record =
                action: 'publish'
                channel: channel
                message:
                  uuid: uuid.v4()
              message = JSON.stringify record
              #console.log "Publishing to channel '#{channel}' message : #{message}"
              ws.send message
              published += 1

              timedSummary()

              setTimeout sendMessage, publishDelay
          catch error
            console.error "[#{process.pid}] Error publishing message:", error

        sendMessage()

      ws.once 'close', ->
        closed = true
        active -= 1
        console.log "[#{process.pid}] Publisher #{id} to channel '#{channel}' disconnected"
        finalSummary()

        if active < 1
          resolve summary

      ws.on 'error', (error) ->
        console.error "[#{process.pid}] Publisher #{id} to channel '#{channel}' - error:", error

      webSockets.push ws

    closeWebSockets = () ->
      console.log "[#{process.pid}] Closing WebSockets after #{watch}"
      webSockets.forEach (ws) ->
        try
          ws.close()
        catch error
          console.error "[#{process.pid}] Error closing WebSocket:", error


# Run an individual subscriber
runSubWorker = ({id, subscriberGroup}) ->
  return new P (resolve, reject) ->
    console.log "#{id} subscriberGroup:", subscriberGroup
    webSockets = []
    active = 0
    received = 0
    summary = {}
    watch = durations.stopwatch().start()

    lastSummaryWatch = durations.stopwatch().start()

    # Summarize all of the message deliveries
    summarize = ->
      console.log "[#{process.pid}] Test ran for #{watch}
        \n  received=#{received}"

    timedSummary = ->
      if lastSummaryWatch.duration().millis() >= 1000
        summarize()
        lastSummaryWatch.reset().start()

    finalSummary = -> summarize() if active < 1

    channels.forEach (channel) ->
      subscriberGroup.forEach (i) ->
        ws = new WebSocket(uri)

        ws.once 'open', ->
          subscription =
            action: 'subscribe'
            channel: channel
          ws.send JSON.stringify(subscription)

          active += 1
          watch.start()
          setTimeout closeWebSockets, runDuration
          console.log "[#{process.pid}] Subscriber #{i} to channel '#{channel}' connected"

        ws.on 'message', (message) ->
          #console.log "Subscriber #{i} to channel '#{channel}' received message : #{message}"
          received += 1
          timedSummary()
          #if watch.duration().millis() > runDuration
          #  ws.close()

        ws.once 'close', ->
          active -= 1
          console.log "[#{process.pid}] Subscriber #{i} to channel '#{channel}' disconnected"
          finalSummary()

          if active < 1
            resolve summary

        ws.on 'error', (error) ->
          console.error "[#{process.pid}] Subscriber #{i} to channel '#{channel}' - error:", error

        webSockets.push ws

    closeWebSockets = () ->
      console.log "[#{process.pid}] Closing WebSockets after #{watch}"
      webSockets.forEach (ws) ->
        try
          ws.close()
        catch error
          console.error "[#{process.pid}] Error closing WebSocket:", error

summaries =
  sub: []
  pub: []

# Run all of the publisher processes
runPubWorkers = ->
  if cluster.isMaster
    chunks = _.chunk(channels, pubWorkers)
    channelGroups = _.zip(chunks...)

    console.log "[#{process.pid}] Channel Groups:
      #{JSON.stringify(channelGroups)}"

    [0...pubWorkers].forEach (i) ->
      worker = cluster.fork()
      worker.on 'message', (msg) ->
        console.log "[#{process.pid}] Message from pub-worker
          #{worker.process.pid} [#{i}]:
          #{JSON.stringify(msg)}"

        switch msg.action
          when 'get-channels'
            worker.send({
              id: "pub-#{i}",
              channelGroup: channelGroups[i]
            })
          when 'summary'
            summaries.pub.push msg.summary

    cluster.on 'exit', (worker, code, signal) ->
      console.log "[#{process.pid}] Pub-worker #{worker.process.pid} halted"
  else
    process.on 'message', (data) ->
      console.log "[#{process.pid}] Pub-worker got data: #{JSON.stringify(data)}"

      if _.startsWith(data.id, 'pub-')
        runPubWorker(data)
        .then (summary) ->
          process.send({
            action: 'summary'
            summary: summary
          })

    process.send({action: 'get-channels'})

# Run all of the subscriber processes
runSubWorkers = ->
  if cluster.isMaster
    subscribers = [1..subscriberCount]
    chunks = _.chunk(subscribers, subWorkers)
    subscriberGroups = _.zip(chunks...)

    console.log "[#{process.pid}] Subscriber Groups:
      #{JSON.stringify(subscriberGroups)}"

    [0...pubWorkers].forEach (i) ->
      worker = cluster.fork()
      worker.on 'message', (msg) ->
        console.log "[#{process.pid}] Message from sub-worker
          #{worker.process.pid} [#{i}]:
          #{JSON.stringify(msg)}"

        switch msg.action
          when 'get-subscribers'
            worker.send({
              id: "sub-#{i}",
              subscriberGroup: subscriberGroups[i]
            })
          when 'summary'
            summaries.sub.push msg.summary

    cluster.on 'exit', (worker, code, signal) ->
      console.log "[#{process.pid}] Sub-worker #{worker.process.pid} halted"
  else
    process.on 'message', (data) ->
      console.log "[#{process.pid}] Sub-worker got data: #{JSON.stringify(data)}"

      if _.startsWith(data.id, 'sub-')
        runSubWorker(data)
        .then (summary) ->
          process.send({
            action: 'summary'
            summary: summary
          })

    process.send({action: 'get-subscribers'})

runSubWorkers()
runPubWorkers()

