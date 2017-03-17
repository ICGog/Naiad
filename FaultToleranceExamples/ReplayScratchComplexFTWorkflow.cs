/*
 * Naiad ver. 0.6
 * Copyright (c) Ionel Gog
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.BatchEntry;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;
using Microsoft.Research.Naiad.Serialization;

using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;

namespace FaultToleranceExamples.ReplayScratchComplexFTWorkflow
{

  internal static class ExtensionMethods
  {
    private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(
      Checkpoint checkpoint, LexStamp time, ReplayScratchComplexFTWorkflow replayWorkflow) {

      if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(time))
      {
        return new Checkpoint(
          checkpoint.node,
          LexStamp.SetBelow(time, replayWorkflow.StageLenghts[checkpoint.node.DenseStageId]),
          true).PairWith(time);
      }
      else
      {
        return checkpoint.PairWith(time);
      }
    }

    public static Stream<Frontier, T> ReduceForDiscarded<T>(
      this Stream<Frontier, T> frontiers,
      Stream<Checkpoint, T> checkpoints,
      Stream<DiscardedMessage, T> discardedMessages,
      ReplayScratchComplexFTWorkflow replayWorkflow) where T : Time<T>
    {
      return frontiers
        // only take the restoration frontiers
        .Where(f => !f.isNotification)
        // only take the lowest frontier at each stage
        .Min2(f => f.node.DenseStageId, f => f.frontier.value)
        // match with all the discarded messages to the node for a given restoration frontier
        .Join(discardedMessages, f => f.Second.node.DenseStageId, m => m.dstDenseStage, (f, m) => f.Second.PairWith(m))
        // keep all discarded messages that are outside the restoration frontier at the node
        .Where(p => !p.First.frontier.Contains(p.Second.dstTime))
        // we only need the sender node id and the send time of the discarded message
        .Select(p => p.Second.src.PairWith(p.Second.srcTime))
        // keep the sender node and minimum send time of any discarded message outside its destination restoration frontier
        .Min2(m => m.First.denseId, m => m.Second.value)
        // for each node that sent a needed discarded message, match it up with all the available checkpoints,
        // reducing downward-closed checkpoints to be less than the time the message was sent
        .Join(
              checkpoints, m => m.Second.First.denseId, c => c.node.denseId,
              (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second.Second, replayWorkflow))
        // then throw out any checkpoints that included any required but discarded sent messages
        .Where(c => !c.First.checkpoint.Contains(c.Second))
        // and just keep the feasible checkpoint
        .Select(c => c.First)
        // now select the largest feasible checkpoint at each node constrained by discarded messages
        .Max2(c => c.node.denseId, c => c.checkpoint.value)
        // and convert it to a pair of frontiers
        .SelectMany(c => new Frontier[] {
            new Frontier(c.Second.node, c.Second.checkpoint, false),
            new Frontier(c.Second.node, c.Second.checkpoint, true) });
    }

    private static Pair<UInt64, UInt64> StageFrontierKey(Pair<Pair<int, SV>, LexStamp> sf)
    {
      UInt64 stages = (((UInt64)sf.First.First) << 48) +
        (((UInt64)sf.First.Second.denseId) << 32);
      UInt64 value = (sf.Second.value < 0) ?
        (UInt64)0xffffffffffffffff :
        (UInt64)sf.Second.value;
      return stages.PairWith(value);
    }

    private static int EdgeKey(Pair<int, SV> edge)
    {
      return (edge.First << 16) + edge.Second.denseId;
    }

    public static Stream<Frontier, T> Reduce<T>(
      this Stream<Frontier, T> frontiers,
      Stream<Checkpoint, T> checkpoints,
      Stream<DeliveredMessage, T> deliveredMessageTimes,
      Stream<Notification, T> deliveredNotificationTimes,
      Stream<Edge, T> graph,
      ReplayScratchComplexFTWorkflow replayWorkflow) where T : Time<T>
    {
      Stream<Pair<Pair<int, SV>, LexStamp>, T> projectedMessageFrontiers = frontiers
        // only look at the restoration frontiers
        .Where(f => !f.isNotification)
        // project each frontier along each outgoing edge
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => e.src.DenseStageId
            .PairWith(e.dst)
            .PairWith(f.frontier.Project(
               replayWorkflow.StageTypes[e.src.DenseStageId],
               replayWorkflow.StageLenghts[e.src.DenseStageId])))
        // keep only the lowest projected frontier from each src stage
        .Min2(f => StageFrontierKey(f), f => f.Second.value)
        .Select(f => f.Second);

      Stream<Pair<SV,LexStamp>,T> staleDeliveredMessages = deliveredMessageTimes
        //// make sure messages are unique
        //.Distinct()
        // match up delivered messages with the projected frontier along the delivery edge,
        // keeping the dst node, dst time and projected frontier
        .Join(
          projectedMessageFrontiers, m => EdgeKey(m.srcDenseStage.PairWith(m.dst)), f => EdgeKey(f.First),
                (m, f) => f.First.Second.PairWith(m.dstTime.PairWith(f.Second)))
          // filter to keep only messages that fall outside their projected frontiers
          .Where(m => !m.Second.Second.Contains(m.Second.First))
          // we only care about the destination node and stale message time
          .Select(m => m.First.PairWith(m.Second.First));

      Stream<Frontier, T> intersectedProjectedNotificationFrontiers = frontiers
        // only look at the notification frontiers
        .Where(f => f.isNotification)
        // project each frontier along each outgoing edge to its destination
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => new Frontier(e.dst, f.frontier.Project(
            replayWorkflow.StageTypes[e.src.DenseStageId],
            replayWorkflow.StageLenghts[e.src.DenseStageId]), true))
        // and find the intersection (minimum) of the projections at the destination
        .Min2(f => f.node.denseId, f => f.frontier.value)
        .Select(f => f.Second);

      Stream<Pair<SV,LexStamp>,T> staleDeliveredNotifications = deliveredNotificationTimes
        // match up delivered notifications with the intersected projected notification frontier at the node,
        // keeping node, time and intersected projected frontier
        .Join(
          intersectedProjectedNotificationFrontiers, n => n.node.denseId, f => f.node.denseId,
          (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
        // filter to keep only notifications that fall outside their projected frontiers
        .Where(n => !n.Second.Second.Contains(n.Second.First))
        // we only care about the node and stale notification time
        .Select(n => n.First.PairWith(n.Second.First));

      Stream<Pair<SV,LexStamp>,T> earliestStaleEvents = staleDeliveredMessages
        .Concat(staleDeliveredNotifications)
        // keep only the earliest stale event at each node
        .Min2(n => n.First.denseId, n => n.Second.value)
        .Select(f => f.Second);

      var reducedFrontiers = checkpoints
        // for each node that executed a stale, match it up with all the available checkpoints,
        // reducing downward-closed checkpoints to be less than the time the event happened at
        .Join(
          earliestStaleEvents, c => c.node.denseId, e => e.First.denseId,
          (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second, replayWorkflow))
        // then throw out any checkpoints that included any stale events
        .Where(c => !c.First.checkpoint.Contains(c.Second))
        // and select the largest feasible checkpoint at each node
        .Max2(c => c.First.node.denseId, c => c.First.checkpoint.value)
        .Select(c => c.Second)
        // then convert it to a pair of frontiers
        .SelectMany(c => new Frontier[] {
            new Frontier(c.First.node, c.First.checkpoint, false),
            new Frontier(c.First.node, c.First.checkpoint, true) });

      // return any reduction in either frontier
      return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers);
    }

  }

  public class ReplayScratchComplexFTWorkflow : Example
  {

    Configuration config;

    private List<int> stageLenghts;
    internal List<int> StageLenghts { get {return this.stageLenghts;}}
    private List<int> stageTypes;
    internal List<int> StageTypes { get {return this.stageTypes;}}

    public string Usage { get { return ""; } }

    public string Help
    {
      get { return "Replay complex fault tolerante workflow."; }
    }

    private void ReadInitialOnNext(NaiadReader reader,
                                   List<Checkpoint> checkpointChanges,
                                   List<Notification> notificationChanges,
                                   List<DeliveredMessage> deliveredMessageChanges,
                                   List<DiscardedMessage> discardedMessageChanges)
    {
      int numCheckpointChanges = reader.Read<int>();
      for (int i = 0; i < numCheckpointChanges; i++)
      {
        Checkpoint chpoint = new Checkpoint();
        chpoint.Restore2(reader);
        checkpointChanges.Add(chpoint);
      }
      int numNotificationChanges = reader.Read<int>();
      for (int i = 0; i < numNotificationChanges; i++)
      {
        Notification notif = new Notification();
        notif.Restore2(reader);
        notificationChanges.Add(notif);
      }
      int numDeliveredMessageChanges = reader.Read<int>();
      for (int i = 0; i < numDeliveredMessageChanges; i++)
      {
        DeliveredMessage deliverMsg = new DeliveredMessage();
        deliverMsg.Restore2(reader);
        deliveredMessageChanges.Add(deliverMsg);
      }
      int numDiscardedMessageChanges = reader.Read<int>();
      for (int i = 0; i < numDiscardedMessageChanges; i++)
      {
        DiscardedMessage discardMsg = new DiscardedMessage();
        discardMsg.Restore2(reader);
        discardedMessageChanges.Add(discardMsg);
      }
    }

    private bool ReadOnNext(NaiadReader reader,
                            List<Weighted<Checkpoint>> checkpointChanges,
                            List<Weighted<Notification>> notificationChanges,
                            List<Weighted<DeliveredMessage>> deliveredMessageChanges,
                            List<Weighted<DiscardedMessage>> discardedMessageChanges)
    {
      int numCheckpointChanges = reader.Read<int>();
      for (int i = 0; i < numCheckpointChanges; i++)
      {
        Checkpoint chpoint = new Checkpoint();
        chpoint.Restore2(reader);
        checkpointChanges.Add(new Weighted<Checkpoint>(chpoint, reader.Read<Int64>()));
      }
      int numNotificationChanges = reader.Read<int>();
      for (int i = 0; i < numNotificationChanges; i++)
      {
        Notification notif = new Notification();
        notif.Restore2(reader);
        notificationChanges.Add(new Weighted<Notification>(notif, reader.Read<Int64>()));
      }
      int numDeliveredMessageChanges = reader.Read<int>();
      for (int i = 0; i < numDeliveredMessageChanges; i++)
      {
        DeliveredMessage deliverMsg = new DeliveredMessage();
        deliverMsg.Restore2(reader);
        deliveredMessageChanges.Add(new Weighted<DeliveredMessage>(deliverMsg, reader.Read<Int64>()));
      }
      int numDiscardedMessageChanges = reader.Read<int>();
      for (int i = 0; i < numDiscardedMessageChanges; i++)
      {
        DiscardedMessage discardMsg = new DiscardedMessage();
        discardMsg.Restore2(reader);
        discardedMessageChanges.Add(new Weighted<DiscardedMessage>(discardMsg, reader.Read<Int64>()));
      }
      if (numCheckpointChanges == -1 &&
          numNotificationChanges == -1 &&
          numDeliveredMessageChanges == -1 &&
          numDiscardedMessageChanges == -1) {
        return false;
      } else
      {
        return true;
      }
    }

    HashSet<Checkpoint> checkpointState;
    HashSet<Notification> notificationState;
    HashSet<DeliveredMessage> delivMsgState;
    HashSet<DiscardedMessage> discMsgState;

    private void ApplyInitialDeltas(List<Checkpoint> checkpointChanges,
                                    List<Notification> notificationChanges,
                                    List<DeliveredMessage> delivMessageChanges,
                                    List<DiscardedMessage> discMessageChanges)
    {
      foreach (Checkpoint checkpoint in checkpointChanges)
      {
        var added = checkpointState.Add(checkpoint);
        if (added == false)
        {
          throw new ApplicationException("Already exists " + checkpoint);
        }
      }
      foreach (Notification notification in notificationChanges)
      {
        var added = notificationState.Add(notification);
        if (added == false)
        {
          throw new ApplicationException("Already exists " + notification);
        }
      }
      foreach (DeliveredMessage delivMsg in delivMessageChanges)
      {
        var added = delivMsgState.Add(delivMsg);
        if (added == false)
        {
          throw new ApplicationException("Already exists " + delivMsg);
        }
      }
      foreach (DiscardedMessage discMsg in discMessageChanges)
      {
        var added = discMsgState.Add(discMsg);
        if (added == false)
        {
          throw new ApplicationException("Already exists " + discMsg);
        }
      }
    }

    private void ApplyDeltas(List<Weighted<Checkpoint>> checkpointChanges,
                             List<Weighted<Notification>> notificationChanges,
                             List<Weighted<DeliveredMessage>> deliveredMessageChanges,
                             List<Weighted<DiscardedMessage>> discardedMessageChanges)
    {
      foreach (Weighted<Checkpoint> checkpoint in checkpointChanges)
      {
        if (checkpoint.weight == -1)
        {
          var removed = checkpointState.Remove(checkpoint.record);
          if (removed == false)
          {
            throw new ApplicationException("Does not exist " + checkpoint);
          }
        }
        else if (checkpoint.weight == 1)
        {
          var added = checkpointState.Add(checkpoint.record);
          if (added == false)
          {
            throw new ApplicationException("Already exists " + checkpoint);
          }
        }
        else
        {
          throw new ApplicationException("Unexpected weight");
        }
      }

      foreach (Weighted<Notification> notification in notificationChanges)
      {
        if (notification.weight == -1)
        {
          var removed = notificationState.Remove(notification.record);
          if (removed == false)
          {
            throw new ApplicationException("Does not exist " + notification);
          }
        }
        else if (notification.weight == 1)
        {
          var added = notificationState.Add(notification.record);
          if (added == false)
          {
            throw new ApplicationException("Already exists " + notification);
          }
        }
        else
        {
          throw new ApplicationException("Unexpected weight");
        }
      }

      foreach (Weighted<DeliveredMessage> delivMsg in deliveredMessageChanges)
      {
        if (delivMsg.weight == -1)
        {
          var removed = delivMsgState.Remove(delivMsg.record);
          if (removed == false)
          {
            throw new ApplicationException("Does not exist " + delivMsg);
          }
        }
        else if (delivMsg.weight == 1)
        {
          var added = delivMsgState.Add(delivMsg.record);
          if (added == false)
          {
            throw new ApplicationException("Already exists " + delivMsg);
          }
        }
        else
        {
          throw new ApplicationException("Unexpected weight");
        }
      }

      foreach (Weighted<DiscardedMessage> discMsg in discardedMessageChanges)
      {
        if (discMsg.weight == -1)
        {
          var removed = discMsgState.Remove(discMsg.record);
          if (removed == false)
          {
            throw new ApplicationException("Does not exist " + discMsg);
          }
        }
        else if (discMsg.weight == 1)
        {
          var added = discMsgState.Add(discMsg.record);
          if (added == false)
          {
            throw new ApplicationException("Already exists " + discMsg);
          }
        }
        else
        {
          throw new ApplicationException("Unexpected weight");
        }
      }
    }

    public void Execute(string[] args)
    {
      this.config = Configuration.FromArgs(ref args);
      this.config.MaxLatticeInternStaleTimes = 10;
      string onNextGraphFile = "/tmp/falkirk/onNextGraph.log";
      string onNextFile = "/tmp/falkirk/onNext.log";
      int curEpoch = 0;
      int replayNumEpochs = -1;
      int argIndex = 1;
      while (argIndex < args.Length)
      {
        switch (args[argIndex].ToLower())
        {
          case "-onnextgraphfile":
            onNextGraphFile = args[argIndex + 1];
            argIndex += 2;
            break;
          case "-onnextfile":
            onNextFile = args[argIndex + 1];
            argIndex += 2;
            break;
          case "-replaynumepochs":
            replayNumEpochs = Int32.Parse(args[argIndex + 1]);
            argIndex += 2;
            break;
          default:
            throw new ApplicationException("Unknown argument " + args[argIndex]);
        }
      }

      this.stageLenghts = new List<int>();
      this.stageTypes = new List<int>();

      this.checkpointState = new HashSet<Checkpoint>();
      this.notificationState = new HashSet<Notification>();
      this.delivMsgState = new HashSet<DeliveredMessage>();
      this.discMsgState = new HashSet<DiscardedMessage>();

      using (var computation = NewComputation.FromConfig(this.config))
      {
        BatchedDataSource<Edge> graphInput = new BatchedDataSource<Edge>();
        BatchedDataSource<Checkpoint> checkpointStreamInput = new BatchedDataSource<Checkpoint>();
        BatchedDataSource<DeliveredMessage> deliveredMessagesInput = new BatchedDataSource<DeliveredMessage>();
        BatchedDataSource<Notification> deliveredNotificationsInput = new BatchedDataSource<Notification>();
        BatchedDataSource<DiscardedMessage> discardedMessagesInput = new BatchedDataSource<DiscardedMessage>();

        var graph = computation.NewInput(graphInput);
        var checkpointStream = computation.NewInput(checkpointStreamInput);
        var deliveredMessages = computation.NewInput(deliveredMessagesInput);
        var deliveredNotifications = computation.NewInput(deliveredNotificationsInput);
        var discardedMessages = computation.NewInput(discardedMessagesInput);

        Stream<Frontier, Epoch> initial = checkpointStream
          .Max2(c => c.node.denseId, c => c.checkpoint.value)
          .SelectMany(c => new Frontier[] {
              new Frontier(c.Second.node, c.Second.checkpoint, false),
              new Frontier(c.Second.node, c.Second.checkpoint, true) });

        var frontiers = initial
          .IterateAndAccumulate((c, f) =>
            {
              var reducedDiscards = f
                .ReduceForDiscarded(c.EnterLoop(checkpointStream),
                                    c.EnterLoop(discardedMessages), this);
              var reduced = f
                .Reduce(c.EnterLoop(checkpointStream),
                        c.EnterLoop(deliveredMessages),
                        c.EnterLoop(deliveredNotifications),
                        c.EnterLoop(graph), this);
              return reduced.Concat(reducedDiscards).Concat(f)
                .Min2(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)), ff => ff.frontier.value)
                .Select(fff => fff.Second);
            }, Int32.MaxValue, "ComputeFrontiers");


        frontiers.Subscribe(l => { } );
        computation.Activate();


        SerializationFormat serFormat =
          SerializationFactory.GetCodeGeneratorForVersion(this.config.SerializerVersion.First,
                                                          this.config.SerializerVersion.Second);

        if (this.config.ProcessID == 0)
        {
          using (FileStream graphStream = File.OpenRead(onNextGraphFile))
          {
            using (NaiadReader onNextGraphReader = new NaiadReader(graphStream, serFormat))
            {
              int numStages = onNextGraphReader.Read<int>();
              for (int i = 0; i < numStages; ++i)
              {
                int stageType = onNextGraphReader.Read<int>();
                stageTypes.Add(stageType);
                int stageLength = onNextGraphReader.Read<int>();
                stageLenghts.Add(stageLength);
              }
              while (true)
              {
                int count = onNextGraphReader.Read<int>();
                if (count == 0)
                {
                  break;
                }
                List<Edge> edges = new List<Edge>();
                for (int i = 0; i < count; i++)
                {
                  Edge edge = new Edge();
                  edge.Restore2(onNextGraphReader);
                  edges.Add(edge);
                }
                graphInput.OnNext(edges);
              }
            }
          }
        }

        graphInput.OnCompleted();

        if (this.config.ProcessID == 0)
        {
          using (FileStream stream = File.OpenRead(onNextFile))
          {
            using (NaiadReader onNextReader = new NaiadReader(stream, serFormat))
            {
              List<Checkpoint> initCheckpointChanges = new List<Checkpoint>();
              List<Notification> initNotificationChanges = new List<Notification>();
              List<DeliveredMessage> initDeliveredMessageChanges = new List<DeliveredMessage>();
              List<DiscardedMessage> initDiscardedMessageChanges = new List<DiscardedMessage>();

              ReadInitialOnNext(onNextReader, initCheckpointChanges,
                                initNotificationChanges,
                                initDeliveredMessageChanges,
                                initDiscardedMessageChanges);

              ApplyInitialDeltas(initCheckpointChanges,
                                 initNotificationChanges,
                                 initDeliveredMessageChanges,
                                 initDiscardedMessageChanges);

              checkpointStreamInput.OnNext(initCheckpointChanges);
              deliveredNotificationsInput.OnNext(initNotificationChanges);
              deliveredMessagesInput.OnNext(initDeliveredMessageChanges);
              discardedMessagesInput.OnNext(initDiscardedMessageChanges);
              computation.Sync(curEpoch);
              curEpoch++;

              while (curEpoch < replayNumEpochs)
              {
                List<Weighted<Checkpoint>> checkpointChanges = new List<Weighted<Checkpoint>>();
                List<Weighted<Notification>> notificationChanges = new List<Weighted<Notification>>();
                List<Weighted<DeliveredMessage>> deliveredMessageChanges = new List<Weighted<DeliveredMessage>>();
                List<Weighted<DiscardedMessage>> discardedMessageChanges = new List<Weighted<DiscardedMessage>>();

                bool readData = ReadOnNext(onNextReader, checkpointChanges,
                                           notificationChanges,
                                           deliveredMessageChanges,
                                           discardedMessageChanges);
                if (readData)
                {
                  ApplyDeltas(checkpointChanges, notificationChanges, deliveredMessageChanges,
                              discardedMessageChanges);
                  Console.WriteLine("State {0} {1} {2} {3}",
                                    checkpointState.Count,
                                    notificationState.Count,
                                    delivMsgState.Count,
                                    discMsgState.Count);
                  var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                  checkpointStreamInput.OnNext(checkpointState.ToList());
                  deliveredNotificationsInput.OnNext(notificationState.ToList());
                  deliveredMessagesInput.OnNext(delivMsgState.ToList());
                  discardedMessagesInput.OnNext(discMsgState.ToList());
                  computation.Sync(curEpoch);
                  Console.Error.WriteLine("Time to process epoch {0}: {1} {2} {3} {4} {5}", curEpoch, stopwatch.ElapsedMilliseconds, checkpointChanges.Count, notificationChanges.Count, deliveredMessageChanges.Count, discardedMessageChanges.Count);
                  curEpoch++;
                } else
                {
                  break;
                }
              }
            }
          }

          checkpointStreamInput.OnCompleted();
          deliveredMessagesInput.OnCompleted();
          deliveredNotificationsInput.OnCompleted();
          discardedMessagesInput.OnCompleted();

          computation.Join();
        }
      }
    }
  }

}