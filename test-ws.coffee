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
EventEmitter = require 'events'

#uri = 'ws://52.39.3.158:8888'
uri = 'ws://localhost:8888'
runDuration = 20000
channelCount = 10
pubWorkers = 2
subWorkers = 4
subscriberCount = 40
publishDelay = 10

# Run an individual publisher
runPubWorker = ({id, channelGroup}) ->
  console.log "Starting worker #{id}"
  worker = new EventEmitter()

  webSockets = []
  active = 0
  published = 0
  summary =
    id: id
    type: 'publisher'

  watch = durations.stopwatch()
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

  totalWs = channelGroup.length

  # Set up a WebSocket for each channel
  channelGroup.forEach (channel) ->
    closed = false
    ws = new WebSocket(uri)

    ws.once 'open', ->
      active += 1
      console.log "[#{process.pid}] Publisher #{id} to channel '#{channel}' connected (#{active} of #{totalWs})"

      if active == totalWs
        process.nextTick -> worker.emit('all-ws-connected')

      sendMessage = () ->
        try
          if not closed
            record =
              action: 'publish'
              channel: channel
              message:
                uuid: uuid.v4()
            message = JSON.stringify record

            ws.send message
            published += 1

            timedSummary()

            setTimeout sendMessage, publishDelay
        catch error
          console.error "[#{process.pid}] Error publishing message:", error

      worker.once 'start', ->
        sendMessage()

    ws.once 'close', ->
      closed = true
      active -= 1
      console.log "[#{process.pid}] Publisher #{id} to channel '#{channel}' disconnected (#{active} of #{totalWs})"
      finalSummary()

      if active < 1
        summary.published = published
        process.nextTick -> worker.emit('all-ws-disconnected')
        process.nextTick -> worker.emit('worker-summary', summary)

    ws.on 'error', (error) ->
      console.error "[#{process.pid}] Publisher #{id} to channel '#{channel}' - error:", error

    webSockets.push ws

  # Termination function for all WebSockets
  closeWebSockets = () ->
    console.log "[#{process.pid}] Closing worker #{id} WebSockets
      after #{watch}"
    webSockets.forEach (ws) ->
      try
        ws.close()
      catch error
        console.error "[#{process.pid}] Error closing WebSocket:", error

  # Set the termination timeout once we get the 'start' signal
  worker.once 'start', ->
    watch.start()
    setTimeout closeWebSockets, runDuration

  worker

# Run an individual subscriber
runSubWorker = ({id, subscriberGroup, channels}) ->
  console.log "Starting worker #{id}"
  worker = new EventEmitter()

  console.log "#{id} subscriberGroup:", subscriberGroup
  webSockets = []
  active = 0
  received = 0
  summary =
    id: id
    type: 'subscriber'
  watch = durations.stopwatch()

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

  totalWs = channels.length * subscriberGroup.length

  channels.forEach (channel) ->
    subscriberGroup.forEach (i) ->
      ws = new WebSocket(uri)

      ws.once 'open', ->
        active += 1
        console.log "[#{process.pid}] Subscriber #{i} to channel '#{channel}' connected (#{active} of #{totalWs})"

        subscription =
          action: 'subscribe'
          channel: channel
        ws.send JSON.stringify(subscription)

        if active >= totalWs
          process.nextTick -> worker.emit('all-ws-connected')

      ws.on 'message', (message) ->
        #console.log "Subscriber #{i} to channel '#{channel}' received message : #{message}"
        received += 1
        timedSummary()

      ws.once 'close', ->
        active -= 1
        console.log "[#{process.pid}] Subscriber #{i} to channel '#{channel}' disconnected (#{active} of #{totalWs})"
        finalSummary()

        if active < 1
          summary.received = received
          process.nextTick -> worker.emit('all-ws-disconnected')
          process.nextTick -> worker.emit('worker-summary', summary)

      ws.on 'error', (error) ->
        console.error "[#{process.pid}] Subscriber #{i} to channel '#{channel}' - error:", error

      webSockets.push ws

  closeWebSockets = () ->
    console.log "[#{process.pid}] Closing worker #{id} WebSockets
      after #{watch}"

    webSockets.forEach (ws) ->
      try
        ws.close()
      catch error
        console.error "[#{process.pid}] Error closing WebSocket:", error

  # Set the termination timeout once we get the 'start' signal
  worker.once 'start', ->
    watch.start()
    setTimeout closeWebSockets, runDuration

  worker

summaries =
  sub: []
  pub: []

# Master process
runMaster = ->
  channels = [1..channelCount].map -> uuid.v4()
  jobs = []
  workers = []
  summaries = []
  connectedWorkers = 0
  disconnectedWorkers = 0

  # Split up channels between publish workers
  chunks = _.chunk(channels, pubWorkers)
  channelGroups = _.zip(chunks...)

  console.log "[#{process.pid}] Channel Groups:
    #{JSON.stringify(channelGroups)}"
   
  # Add publisher jobs
  [0...pubWorkers].forEach (i) ->
    jobs.push
      id: "pub-#{i}"
      type: "publisher"
      channelGroup: channelGroups[i]
  
  # Set up subscriber workers
  subscribers = [1..subscriberCount]
  chunks = _.chunk(subscribers, subWorkers)
  subscriberGroups = _.zip(chunks...)

  console.log "[#{process.pid}] Subscriber Groups:
    #{JSON.stringify(subscriberGroups)}"

  # Add subscriber jobs
  [0...subWorkers].forEach (i) ->
    jobs.push
      id: "sub-#{i}",
      type: "subscriber"
      channels: channels
      subscriberGroup: subscriberGroups[i]

  console.log "[#{process.pid}] jobs:\n#{JSON.stringify(jobs, null, 2)}"

  # Fork a new worker process for each job
  jobs.forEach (job) ->
    worker = cluster.fork()
    workers.push worker

    # Handle messages from workers
    messageHandler = (msg) ->
      switch msg.action
        # Worker is ready to receive its job
        when 'worker-ready'
          console.log "[#{process.pid}] sending job to worker #{job.id}"
          worker.send
            action: 'job'
            job: job
        # Worker has established all of its WebSockets and is ready to start
        when 'all-ws-connected'
          console.log "[#{process.pid}] worker #{job.id} all ws connected"
          connectedWorkers += 1
          if connectedWorkers == workers.length
            workers.forEach (worker) ->
              worker.send
                action: 'start'
        # Worker has terminated all of its WebSockets and is ready to halt
        when 'all-ws-disconnected'
          console.log "[#{process.pid}] worker #{job.id} all ws disconnected"
          disconnectedWorkers += 1

          if disconnectedWorkers == workers.length
            console.log "All workers' WebSockets disconnected."
        # Worker summary supplied
        when 'worker-summary'
          worker.removeListener 'message', messageHandler
          console.log "[#{process.pid}]
            received summary from worker #{job.id}:
            #{JSON.stringify(msg.summary)}"

          summaries.push msg.summary

          if summaries.length == workers.length
            console.log "Summaries:", summaries

            received = summaries
              .filter (sum) -> sum.type == 'subscriber'
              .map ({received}) -> received
              .reduce (v, acc) -> v + acc

            published = summaries
              .filter (sum) -> sum.type == 'publisher'
              .map ({published}) -> published
              .reduce (v, acc) -> v + acc

            console.log "  total published: #{published}"
            console.log "   total received: #{received}"

            process.exit 0

    worker.on 'message', messageHandler

  cluster.on 'exit', (worker, code, signal) ->
    console.log "[#{process.pid}] Pub-worker #{worker.process.pid} halted"

# Worker process
runWorker = ->
  console.log "[#{process.pid}] worker started"
  worker = {}

  process.on 'message', (msg) ->
    switch msg.action
      when 'job'
        console.log "[#{process.pid}] worker #{msg.job.id} received its job:
          #{JSON.stringify(msg.job)}"

        job = msg.job
        worker.id = msg.job.id
        runSub = () -> runSubWorker job
        runPub = () -> runPubWorker job
        worker.events = if job.type == 'subscriber' then runSub() else runPub()
        
        worker.events.once 'all-ws-connected', -> 
          process.send
            action: 'all-ws-connected'

        worker.events.once 'all-ws-disconnected', ->
          process.send
            action: 'all-ws-disconnected'

        worker.events.once 'worker-summary', (summary) ->
          process.send
            action: 'worker-summary'
            summary: summary
      when 'start'
        console.log "Starting worker #{worker.id}"
        worker.events.emit 'start'

  process.send
    action: 'worker-ready'

# Entry point
runWorkers = ->
  if cluster.isMaster
    runMaster()
  else
    runWorker()

runWorkers()

