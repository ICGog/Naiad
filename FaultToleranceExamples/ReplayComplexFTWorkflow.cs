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
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;
using Microsoft.Research.Naiad.Serialization;

using Microsoft.Research.Naiad.Examples.DifferentialDataflow;

namespace FaultToleranceExamples.ReplayComplexFTWorkflow
{

  internal static class ExtensionMethods
  {

    private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(
      Checkpoint checkpoint, LexStamp time, ReplayComplexFTWorkflow replayWorkflow) {

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

    public static Collection<Frontier, T> ReduceForDiscarded<T>(
      this Collection<Frontier, T> frontiers,
      Collection<Checkpoint, T> checkpoints,
      Collection<DiscardedMessage, T> discardedMessages,
      ReplayComplexFTWorkflow replayWorkflow) where T : Time<T>
    {
      return frontiers
        // only take the restoration frontiers
        .Where(f => !f.isNotification)
        // only take the lowest frontier at each stage
        .Min(f => f.node.DenseStageId, f => f.frontier.value)
        // match with all the discarded messages to the node for a given restoration frontier
        .Join(discardedMessages, f => f.node.DenseStageId, m => m.dstDenseStage, (f, m) => f.PairWith(m))
        // keep all discarded messages that are outside the restoration frontier at the node
        .Where(p => !p.First.frontier.Contains(p.Second.dstTime))
        // we only need the sender node id and the send time of the discarded message
        .Select(p => p.Second.src.PairWith(p.Second.srcTime))
        // keep the sender node and minimum send time of any discarded message outside its destination restoration frontier
        .Min(m => m.First.denseId, m => m.Second.value)
        // for each node that sent a needed discarded message, match it up with all the available checkpoints,
        // reducing downward-closed checkpoints to be less than the time the message was sent
        .Join(
              checkpoints, m => m.First.denseId, c => c.node.denseId,
              (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second, replayWorkflow))
        // then throw out any checkpoints that included any required but discarded sent messages
        .Where(c => !c.First.checkpoint.Contains(c.Second))
        // and just keep the feasible checkpoint
        .Select(c => c.First)
        // now select the largest feasible checkpoint at each node constrained by discarded messages
        .Max(c => c.node.denseId, c => c.checkpoint.value)
        // and convert it to a pair of frontiers
        .SelectMany(c => new Frontier[] {
            new Frontier(c.node, c.checkpoint, false),
            new Frontier(c.node, c.checkpoint, true) });
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

    public static Collection<Frontier, T> Reduce<T>(
      this Collection<Frontier, T> frontiers,
      Collection<Checkpoint, T> checkpoints,
      Collection<DeliveredMessage, T> deliveredMessageTimes,
      Collection<Notification, T> deliveredNotificationTimes,
      Collection<Edge, T> graph,
      ReplayComplexFTWorkflow replayWorkflow) where T : Time<T>
    {
      Collection<Pair<Pair<int, SV>, LexStamp>, T> projectedMessageFrontiers = frontiers
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
        .Min(f => StageFrontierKey(f), f => f.Second.value);

      Collection<Pair<SV,LexStamp>,T> staleDeliveredMessages = deliveredMessageTimes
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

      Collection<Frontier, T> intersectedProjectedNotificationFrontiers = frontiers
        // only look at the notification frontiers
        .Where(f => f.isNotification)
        // project each frontier along each outgoing edge to its destination
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => new Frontier(e.dst, f.frontier.Project(
            replayWorkflow.StageTypes[e.src.DenseStageId],
            replayWorkflow.StageLenghts[e.src.DenseStageId]), true))
        // and find the intersection (minimum) of the projections at the destination
        .Min(f => f.node.denseId, f => f.frontier.value);

      Collection<Pair<SV,LexStamp>,T> staleDeliveredNotifications = deliveredNotificationTimes
        // match up delivered notifications with the intersected projected notification frontier at the node,
        // keeping node, time and intersected projected frontier
        .Join(
          intersectedProjectedNotificationFrontiers, n => n.node.denseId, f => f.node.denseId,
          (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
        // filter to keep only notifications that fall outside their projected frontiers
        .Where(n => !n.Second.Second.Contains(n.Second.First))
        // we only care about the node and stale notification time
        .Select(n => n.First.PairWith(n.Second.First));

      Collection<Pair<SV,LexStamp>,T> earliestStaleEvents = staleDeliveredMessages
        .Concat(staleDeliveredNotifications)
        // keep only the earliest stale event at each node
        .Min(n => n.First.denseId, n => n.Second.value);

      var reducedFrontiers = checkpoints
        // for each node that executed a stale, match it up with all the available checkpoints,
        // reducing downward-closed checkpoints to be less than the time the event happened at
        .Join(
          earliestStaleEvents, c => c.node.denseId, e => e.First.denseId,
          (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second, replayWorkflow))
        // then throw out any checkpoints that included any stale events
        .Where(c => !c.First.checkpoint.Contains(c.Second))
        // and select the largest feasible checkpoint at each node
        .Max(c => c.First.node.denseId, c => c.First.checkpoint.value)
        // then convert it to a pair of frontiers
        .SelectMany(c => new Frontier[] {
            new Frontier(c.First.node, c.First.checkpoint, false),
            new Frontier(c.First.node, c.First.checkpoint, true) });

      // return any reduction in either frontier
      return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers);
    }

  }

  public class ReplayComplexFTWorkflow : Example
  {

    Configuration config;

    private List<int> stageLenghts;
    internal List<int> StageLenghts { get {return this.stageLenghts;}}
    private List<int> stageTypes;
    internal List<int> StageTypes { get {return this.StageTypes;}}

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
        Console.WriteLine("AAA: {0}", chpoint);
      }
      int numNotificationChanges = reader.Read<int>();
      for (int i = 0; i < numNotificationChanges; i++)
      {
        Notification notif = new Notification();
        notif.Restore2(reader);
        notificationChanges.Add(notif);
        Console.WriteLine("BBB: {0}", notif);
      }
      int numDeliveredMessageChanges = reader.Read<int>();
      for (int i = 0; i < numDeliveredMessageChanges; i++)
      {
        DeliveredMessage deliverMsg = new DeliveredMessage();
        deliverMsg.Restore2(reader);
        deliveredMessageChanges.Add(deliverMsg);
        Console.WriteLine("CCC: {0}", deliverMsg);
      }
      int numDiscardedMessageChanges = reader.Read<int>();
      for (int i = 0; i < numDiscardedMessageChanges; i++)
      {
        DiscardedMessage discardMsg = new DiscardedMessage();
        discardMsg.Restore2(reader);
        discardedMessageChanges.Add(discardMsg);
        Console.WriteLine("DDD: {0}", discardMsg);
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
      if (numCheckpointChanges == 0 &&
          numNotificationChanges == 0 &&
          numDeliveredMessageChanges == 0 &&
          numDiscardedMessageChanges == 0) {
        return false;
      } else
      {
        return true;
      }
    }

    public void Execute(string[] args)
    {
      this.config = Configuration.FromArgs(ref args);
      this.config.MaxLatticeInternStaleTimes = 10;

      this.stageLenghts = new List<int>();
      this.stageTypes = new List<int>();

      using (FileStream stream = File.OpenRead("/tmp/falkirk/onNext.log"))
      {
        SerializationFormat serFormat =
          SerializationFactory.GetCodeGeneratorForVersion(this.config.SerializerVersion.First,
                                                          this.config.SerializerVersion.Second);
        using (NaiadReader onNextReader = new NaiadReader(stream, serFormat))
        {
          using (var computation = NewComputation.FromConfig(this.config))
          {
            InputCollection<Edge> graph = computation.NewInputCollection<Edge>();
            InputCollection<Checkpoint> checkpointStream = computation.NewInputCollection<Checkpoint>();
            InputCollection<DeliveredMessage> deliveredMessages = computation.NewInputCollection<DeliveredMessage>();
            InputCollection<Notification> deliveredNotifications = computation.NewInputCollection<Notification>();
            InputCollection<DiscardedMessage> discardedMessages = computation.NewInputCollection<DiscardedMessage>();

            Collection<Frontier, Epoch> initial = checkpointStream
              .Max(c => c.node.denseId, c => c.checkpoint.value)
              .SelectMany(c => new Frontier[] {
                  new Frontier(c.node, c.checkpoint, false),
                  new Frontier(c.node, c.checkpoint, true) });

            var frontiers = initial
              .FixedPoint((c, f) =>
              {
                var reducedDiscards = f
                    .ReduceForDiscarded(
                       checkpointStream.EnterLoop(c),
                       discardedMessages.EnterLoop(c), this);

                var reduced = f
                    .Reduce(
                       checkpointStream.EnterLoop(c), deliveredMessages.EnterLoop(c),
                       deliveredNotifications.EnterLoop(c), graph.EnterLoop(c),
                       this);

                return reduced.Concat(reducedDiscards).Concat(f)
                       .Min(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)), ff => ff.frontier.value);
              })
              .Consolidate();

            computation.Activate();

            if (this.config.ProcessID == 0)
            {
              using (FileStream graphStream = File.OpenRead("/tmp/falkirk/onNextGraph.log"))
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
                    graph.OnNext(edges);
                  }
                }
              }
            }
            graph.OnCompleted();

            if (this.config.ProcessID == 0)
            {
              List<Checkpoint> initCheckpointChanges = new List<Checkpoint>();
              List<Notification> initNotificationChanges = new List<Notification>();
              List<DeliveredMessage> initDeliveredMessageChanges = new List<DeliveredMessage>();
              List<DiscardedMessage> initDiscardedMessageChanges = new List<DiscardedMessage>();

              ReadInitialOnNext(onNextReader, initCheckpointChanges,
                                initNotificationChanges,
                                initDeliveredMessageChanges,
                                initDiscardedMessageChanges);

              checkpointStream.OnNext(initCheckpointChanges);
              deliveredNotifications.OnNext(initNotificationChanges);
              deliveredMessages.OnNext(initDeliveredMessageChanges);
              discardedMessages.OnNext(initDiscardedMessageChanges);

              int epoch = 1;
              while (true)
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
                  // Console.WriteLine("====================== Epoch {0} ========", epoch);
                  // foreach (Weighted<Checkpoint> checkpoint in checkpointChanges)
                  // {
                  //   Console.WriteLine("AAA: {0}", checkpoint);
                  // }
                  // foreach (Weighted<Notification> notification in notificationChanges)
                  // {
                  //   Console.WriteLine("BBB: {0}", notification);
                  // }
                  // foreach (Weighted<DeliveredMessage> delivMsg in deliveredMessageChanges)
                  // {
                  //   Console.WriteLine("CCC: {0}", delivMsg);
                  // }
                  // foreach (Weighted<DiscardedMessage> dsgMsg in discardedMessageChanges)
                  // {
                  //   Console.WriteLine("DDD: {0}", dsgMsg);
                  // }
                  checkpointStream.OnNext(checkpointChanges);
                  deliveredNotifications.OnNext(notificationChanges);
                  deliveredMessages.OnNext(deliveredMessageChanges);
                  discardedMessages.OnNext(discardedMessageChanges);
                  epoch++;
                  if (epoch == 10)
                  {
                    break;
                  }
                } else
                {
                  break;
                }
              }
            }

            checkpointStream.OnCompleted();
            deliveredMessages.OnCompleted();
            deliveredNotifications.OnCompleted();
            discardedMessages.OnCompleted();

            computation.Join();
          }
        }
      }
    }
  }
}