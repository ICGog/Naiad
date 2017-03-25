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

namespace FaultToleranceExamples.ReplayIncrementalComplexFTWorkflow
{

  public class ReplayIncrementalComplexFTWorkflow : Example
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

    private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(
      Checkpoint checkpoint, LexStamp time, ReplayIncrementalComplexFTWorkflow replayWorkflow) {

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

    private List<Frontier> ReduceForDiscardedScratch(List<Frontier> frontiers,
                                                     List<Checkpoint> checkpoints,
                                                     List<DiscardedMessage> discMsgs)
    {
      return frontiers.Where(f => !f.isNotification)
        // .Min(f => f.node.DenseStageId, f => f.frontier.value)
        .GroupBy(f => f.node.DenseStageId,
                 x => x,
                 (did, fs) => {
                   Frontier minF = fs.First();
                   foreach (var ff in fs)
                   {
                     if (ff.frontier.value < minF.frontier.value)
                     {
                       minF = ff;
                     }
                   }
                   return minF;
                 })
        .Join(discMsgs, f => f.node.DenseStageId, m => m.dstDenseStage, (f, m) => f.PairWith(m))
        .Where(p => !p.First.frontier.Contains(p.Second.dstTime))
        .Select(p => p.Second.src.PairWith(p.Second.srcTime))
        // .Min(m => m.First.denseId, m => m.Second.value)
        .GroupBy(m => m.First.denseId,
                 x => x,
                 (di, ms) => {
                   Pair<SV, LexStamp> minM = ms.First();
                   foreach (var mm in ms)
                   {
                     if (mm.Second.value < minM.Second.value)
                     {
                       minM = mm;
                     }
                   }
                   return minM;
                 })
        .Join(checkpoints, m => m.First.denseId, c => c.node.denseId,
              (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second, this))
        .Where(c => !c.First.checkpoint.Contains(c.Second))
        .Select(c => c.First)
        // .Max(c => c.node.denseId, c => c.checkpoint.value)
        .GroupBy(c => c.node.denseId,
                 x => x,
                 (di, cts) => {
                   Checkpoint maxCT = cts.First();
                   foreach (var ct in cts)
                   {
                     if (ct.checkpoint.value > maxCT.checkpoint.value)
                     {
                       maxCT = ct;
                     }
                   }
                   return maxCT;
                 })
        .SelectMany(c => new Frontier[] {
            new Frontier(c.node, c.checkpoint, false),
            new Frontier(c.node, c.checkpoint, true) }).ToList();
    }

    private List<Frontier> ReduceScratch(System.Diagnostics.Stopwatch stopwatch,List<Frontier> frontiers,
                                         List<Checkpoint> checkpoints,
                                         List<DeliveredMessage> delivMsgs,
                                         List<Notification> notifs,
                                         List<Edge> graph)
    {
      List<Pair<Pair<int, SV>, LexStamp>> projectedMessageFrontiers = frontiers
        .Where(f => !f.isNotification)
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => e.src.DenseStageId
            .PairWith(e.dst)
            .PairWith(f.frontier.Project(
               this.StageTypes[e.src.DenseStageId],
               this.StageLenghts[e.src.DenseStageId])))
      //   .Min(f => StageFrontierKey(f), f => f.Second.value);
        .GroupBy(f => StageFrontierKey(f),
                 x => x,
                 (fKey, pmfs) => {
                   Pair<Pair<int, SV>, LexStamp> minPMF = pmfs.First();
                   foreach (var pmf in pmfs)
                   {
                     if (pmf.Second.value < minPMF.Second.value)
                     {
                       minPMF = pmf;
                     }
                   }
                   return minPMF;
                 }).ToList();

      Console.WriteLine("projectedMessageFrontiers {0} {1}", stopwatch.ElapsedMilliseconds, projectedMessageFrontiers.Count);

      List<Pair<SV,LexStamp>> staleDeliveredMessages = delivMsgs
        .Join(
          projectedMessageFrontiers, m => EdgeKey(m.srcDenseStage.PairWith(m.dst)), f => EdgeKey(f.First),
                (m, f) => f.First.Second.PairWith(m.dstTime.PairWith(f.Second)))
        .Where(m => !m.Second.Second.Contains(m.Second.First))
        .Select(m => m.First.PairWith(m.Second.First)).ToList();

      Console.WriteLine("staleDeliveredMessages {0} {1}", stopwatch.ElapsedMilliseconds, staleDeliveredMessages.Count);

      List<Frontier> intersectedProjectedNotificationFrontiers = frontiers
        .Where(f => f.isNotification)
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => new Frontier(e.dst, f.frontier.Project(
            this.StageTypes[e.src.DenseStageId],
            this.StageLenghts[e.src.DenseStageId]), true))
      //   .Min(f => f.node.denseId, f => f.frontier.value);
        .GroupBy(f => f.node.denseId,
                 x => x,
                 (di, fs) => {
                   Frontier minF = fs.First();
                   foreach (Frontier ff in fs)
                   {
                     if (ff.frontier.value < minF.frontier.value)
                     {
                       minF = ff;
                     }
                   }
                   return minF;
                 }).ToList();

      Console.WriteLine("intersectedProjectedNotificationFrontiers {0} {1}", stopwatch.ElapsedMilliseconds, intersectedProjectedNotificationFrontiers.Count);

      List<Pair<SV,LexStamp>> staleDeliveredNotifications = notifs
        .Join(
          intersectedProjectedNotificationFrontiers, n => n.node.denseId, f => f.node.denseId,
          (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
        .Where(n => !n.Second.Second.Contains(n.Second.First))
        .Select(n => n.First.PairWith(n.Second.First)).ToList();

      Console.WriteLine("staleDeliveredNotifications {0} {1}", stopwatch.ElapsedMilliseconds, staleDeliveredNotifications.Count);

      List<Pair<SV,LexStamp>> earliestStaleEvents = staleDeliveredMessages
        .Concat(staleDeliveredNotifications)
      //   .Min(n => n.First.denseId, n => n.Second.value);
        .GroupBy(n => n.First.denseId,
                 x => x,
                 (di, es) => {
                   Pair<SV,LexStamp> minE = es.First();
                   foreach (var e in es)
                   {
                     if (e.Second.value < minE.Second.value)
                     {
                       minE = e;
                     }
                   }
                   return minE;
                 }).ToList();

      Console.WriteLine("earliestStaleEvents {0} {1}", stopwatch.ElapsedMilliseconds, earliestStaleEvents.Count);

      var reducedFrontiers = checkpoints
        .Join(
          earliestStaleEvents, c => c.node.denseId, e => e.First.denseId,
          (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second, this))
        .Where(c => !c.First.checkpoint.Contains(c.Second))
      //   .Max(c => c.First.node.denseId, c => c.First.checkpoint.value)
        .GroupBy(c => c.First.node.denseId,
                 x => x,
                 (di, cts) => {
                   Pair<Checkpoint, LexStamp> maxCT = cts.First();
                   foreach (var ct in cts)
                   {
                     if (ct.First.checkpoint.value > maxCT.First.checkpoint.value)
                     {
                       maxCT = ct;
                     }
                   }
                   return maxCT;
                 })
        .SelectMany(c => new Frontier[] {
            new Frontier(c.First.node, c.First.checkpoint, false),
            new Frontier(c.First.node, c.First.checkpoint, true) }).ToList();

      Console.WriteLine("reducedFrontiers {0} {1}", stopwatch.ElapsedMilliseconds, reducedFrontiers.Count);

      return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers).ToList();
    }

    public void ComputeFrontiers(List<Weighted<Edge>> graphChanges,
                                 List<Weighted<Checkpoint>> checkpointChanges,
                                 List<Weighted<Notification>> notifChanges,
                                 List<Weighted<DeliveredMessage>> delivMsgChanges,
                                 List<Weighted<DiscardedMessage>> discMsgChanges)
    {
      // TODO(ionel): Implement!
    }

    public List<Frontier> MaxFrontierPerVertex(
        List<Checkpoint> checkpoints)
    {
      var maxCheckpoints =
//      checkpoints.Max(c => c.node.denseId, c => c.checkpoint.value);
        checkpoints.GroupBy(c => c.node.denseId,
                            x => x,
                            (d, cs) => {
                              Checkpoint maxRes = cs.First();
                              foreach (var cc in cs)
                              {
                                if (cc.checkpoint.value > maxRes.checkpoint.value)
                                {
                                  maxRes = cc;
                                }
                              }
                              return maxRes;
                            });
      return maxCheckpoints.SelectMany(c => new Frontier[] {
          new Frontier(c.node, c.checkpoint, false),
          new Frontier(c.node, c.checkpoint, true) }).ToList();
    }

    public List<Frontier> ReduceMicroForDiscarded(
        Frontier curFrontier,
        Dictionary<int, List<DiscardedMessage>> discMsgPerStage,
        Dictionary<int, List<Checkpoint>> checkpointsPerVertex)
    {
      List<DiscardedMessage> discMsgs;
      bool found = discMsgPerStage.TryGetValue(curFrontier.node.DenseStageId, out discMsgs);
      if (!found)
      {
        return new List<Frontier>();
      }
      var minCPerVertex = discMsgs
        .Select(m => curFrontier.PairWith(m))
        .Where(p => !p.First.frontier.Contains(p.Second.dstTime))
        .Select(p => p.Second.src.PairWith(p.Second.srcTime))
        // .Min(m => m.First.denseId, m => m.Second.value)
        .GroupBy(m => m.First.denseId,
                 x => x,
                 (di, ms) => {
                   Pair<SV, LexStamp> minM = ms.First();
                   foreach (var mm in ms)
                   {
                     if (mm.Second.value < minM.Second.value)
                     {
                       minM = mm;
                     }
                   }
                   return minM;
                 }).ToList();
      List<Checkpoint> minCheckpoints = new List<Checkpoint>();
      foreach (var minC in minCPerVertex)
      {
        List<Checkpoint> vChks;
        found = checkpointsPerVertex.TryGetValue(minC.First.denseId, out vChks);
        if (found)
        {
          foreach (Checkpoint checkpoint in vChks)
          {
            var c = PairCheckpointToBeLowerThanTime(checkpoint, minC.Second, this);
            if (!c.First.checkpoint.Contains(c.Second))
            {
              minCheckpoints.Add(c.First);
            }
          }
        }
      }
      return minCheckpoints.GroupBy(
          c => c.node.denseId,
          x => x,
          (di, cts) => {
            Checkpoint maxCT = cts.First();
            foreach (var ct in cts)
            {
              if (ct.checkpoint.value > maxCT.checkpoint.value)
              {
                maxCT = ct;
              }
            }
            return maxCT;
          })
        .SelectMany(c => new Frontier[] {
            new Frontier(c.node, c.checkpoint, false),
            new Frontier(c.node, c.checkpoint, true) }).ToList();
    }

    public List<Frontier> ReduceMicroNotif(Frontier curFrontier,
                                           Dictionary<int, List<Edge>> edgesPerVertex,
                                           Dictionary<int, List<Notification>> notifsPerVertex,
                                           Dictionary<int, List<Checkpoint>> checkpointsPerVertex)
    {
      List<Edge> vEdges = new List<Edge>();
      bool found = edgesPerVertex.TryGetValue(curFrontier.node.denseId, out vEdges);
      List<Frontier> intersectedProjectedNotificationFrontiers = new List<Frontier>();
      if (found)
      {
        intersectedProjectedNotificationFrontiers =
        vEdges.Select(e => new Frontier(e.dst, curFrontier.frontier.Project(
            this.StageTypes[e.src.DenseStageId],
            this.StageLenghts[e.src.DenseStageId]), true))
        .GroupBy(f => f.node.denseId,
                 x => x,
                 (di, fs) => {
                   Frontier minF = fs.First();
                   foreach (Frontier ff in fs)
                   {
                     if (ff.frontier.value < minF.frontier.value)
                     {
                       minF = ff;
                     }
                   }
                   return minF;
                 }).ToList();
      }
      List<Pair<SV,LexStamp>> staleDeliveredNotifications = new List<Pair<SV,LexStamp>>();
      foreach (var intPrjFrontier in intersectedProjectedNotificationFrontiers)
      {
        List<Notification> notifs;
        found = notifsPerVertex.TryGetValue(intPrjFrontier.node.denseId, out notifs);
        if (found)
        {
          foreach (Notification notif in notifs)
          {
            if (!intPrjFrontier.frontier.Contains(notif.time))
            {
              staleDeliveredNotifications.Add(notif.node.PairWith(notif.time));
            }
          }
        }

      }

      var earliestStaleNotifs = staleDeliveredNotifications.GroupBy(n => n.First.denseId,
                                                         x => x,
                                                         (di, es) => {
                                                           Pair<SV,LexStamp> minE = es.First();
                                                           foreach (var e in es)
                                                           {
                                                             if (e.Second.value < minE.Second.value)
                                                             {
                                                               minE = e;
                                                             }
                                                           }
                                                           return minE;
                                                         });

      List<Checkpoint> minCheckpoints = new List<Checkpoint>();
      foreach (var e in earliestStaleNotifs)
      {
        List<Checkpoint> vChks;
        found = checkpointsPerVertex.TryGetValue(e.First.denseId, out vChks);
        if (found)
        {
          foreach (Checkpoint checkpoint in vChks)
          {
            var c = PairCheckpointToBeLowerThanTime(checkpoint, e.Second, this);
            if (!c.First.checkpoint.Contains(c.Second))
            {
              minCheckpoints.Add(c.First);
            }
          }
        }
      }

      return minCheckpoints.GroupBy(c => c.node.denseId,
                             x => x,
                             (di, cts) => {
                               Checkpoint maxCT = cts.First();
                               foreach (var ct in cts)
                               {
                                 if (ct.checkpoint.value > maxCT.checkpoint.value)
                                 {
                                   maxCT = ct;
                                 }
                               }
                               return maxCT;
                             })
        .SelectMany(c => new Frontier[] {
            new Frontier(c.node, c.checkpoint, false),
            new Frontier(c.node, c.checkpoint, true) }).Concat(intersectedProjectedNotificationFrontiers).ToList();
    }

    public List<Frontier> ReduceMicroDeliv(Frontier curFrontier,
                                           Dictionary<int, List<Edge>> edgesPerVertex,
                                           Dictionary<int, List<LexStamp>> delivDstTimePerEdgeKey,
                                           Dictionary<int, List<Checkpoint>> checkpointsPerVertex)
    {
      List<Edge> vEdges = new List<Edge>();
      bool found = edgesPerVertex.TryGetValue(curFrontier.node.denseId, out vEdges);
      List<Pair<Pair<int, SV>, LexStamp>> projectedMessageFrontiers =
        new List<Pair<Pair<int, SV>, LexStamp>>();
      if (found)
      {
        projectedMessageFrontiers =
          vEdges.Select(edge => edge.src.DenseStageId
                        .PairWith(edge.dst)
                        .PairWith(curFrontier.frontier.Project(
                            this.StageTypes[edge.src.DenseStageId],
                            this.StageLenghts[edge.src.DenseStageId])))
          .GroupBy(f => StageFrontierKey(f),
                   x => x,
                   (fKey, pmfs) => {
                     Pair<Pair<int, SV>, LexStamp> minPMF = pmfs.First();
                     foreach (var pmf in pmfs)
                     {
                       if (pmf.Second.value < minPMF.Second.value)
                       {
                         minPMF = pmf;
                       }
                     }
                     return minPMF;
                   }).ToList();
      }
      List<Pair<SV, LexStamp>> staleDeliveredMsgs =
        new List<Pair<SV, LexStamp>>();
      foreach (var prjMsgFrontier in projectedMessageFrontiers)
      {
        List<LexStamp> dstTimes;
        found = delivDstTimePerEdgeKey.TryGetValue(EdgeKey(prjMsgFrontier.First), out dstTimes);
        if (found)
        {
          foreach (LexStamp dstTime in dstTimes)
          {
            if (!prjMsgFrontier.Second.Contains(dstTime))
            {
              staleDeliveredMsgs.Add(prjMsgFrontier.First.Second.PairWith(dstTime));
            }
          }
        }
      }

      var earliestStaleMsgs = staleDeliveredMsgs.GroupBy(n => n.First.denseId,
                                                         x => x,
                                                         (di, es) => {
                                                           Pair<SV,LexStamp> minE = es.First();
                                                           foreach (var e in es)
                                                           {
                                                             if (e.Second.value < minE.Second.value)
                                                             {
                                                               minE = e;
                                                             }
                                                           }
                                                           return minE;
                                                         });

      List<Checkpoint> minCheckpoints = new List<Checkpoint>();
      foreach (var e in earliestStaleMsgs)
      {
        List<Checkpoint> vChks;
        found = checkpointsPerVertex.TryGetValue(e.First.denseId, out vChks);
        if (found)
        {
          foreach (Checkpoint checkpoint in vChks)
          {
            var c = PairCheckpointToBeLowerThanTime(checkpoint, e.Second, this);
            if (!c.First.checkpoint.Contains(c.Second))
            {
              minCheckpoints.Add(c.First);
            }
          }
        }
      }

      return minCheckpoints.GroupBy(c => c.node.denseId,
                             x => x,
                             (di, cts) => {
                               Checkpoint maxCT = cts.First();
                               foreach (var ct in cts)
                               {
                                 if (ct.checkpoint.value > maxCT.checkpoint.value)
                                 {
                                   maxCT = ct;
                                 }
                               }
                               return maxCT;
                             })
        .SelectMany(c => new Frontier[] {
            new Frontier(c.node, c.checkpoint, false),
            new Frontier(c.node, c.checkpoint, true) }).ToList();
    }

    public List<Frontier> ComputeFrontiersMicroBatch(
        System.Diagnostics.Stopwatch stopwatch,
        List<Edge> graph,
        List<Checkpoint> checkpoints,
        List<Notification> notifications,
        List<DeliveredMessage> delivMsgs,
        List<DiscardedMessage> discMsgs)
    {
      var frontiers = MaxFrontierPerVertex(checkpoints);
      int numIterations = 0;
      Dictionary<SV, Frontier> currentFrontiers = new Dictionary<SV, Frontier>();
      Dictionary<SV, Frontier> currentNFrontiers = new Dictionary<SV, Frontier>();
      Dictionary<int, Frontier> minStageFrontier = new Dictionary<int, Frontier>();
      Dictionary<int, bool> doneMinStageFrontier = new Dictionary<int, bool>();
      Dictionary<int, Frontier> minStageNFrontier = new Dictionary<int, Frontier>();
      Stack<Frontier> toProcess = new Stack<Frontier>();
      foreach (Frontier frontier in frontiers)
      {
        toProcess.Push(frontier);
        if (frontier.isNotification)
        {
          currentNFrontiers.Add(frontier.node, frontier);
          Frontier minFrontier;
          bool found = minStageNFrontier.TryGetValue(frontier.node.DenseStageId, out minFrontier);
          if (!found)
          {
            minStageNFrontier.Add(frontier.node.DenseStageId, frontier);
          }
          else
          {
            if (minFrontier.frontier.value > frontier.frontier.value)
            {
              minStageNFrontier[frontier.node.DenseStageId] = frontier;
            }
          }
        }
        else
        {
          currentFrontiers.Add(frontier.node, frontier);
          Frontier minFrontier;
          bool found = minStageFrontier.TryGetValue(frontier.node.DenseStageId, out minFrontier);
          if (!found)
          {
            minStageFrontier.Add(frontier.node.DenseStageId, frontier);
            doneMinStageFrontier.Add(frontier.node.DenseStageId, false);
          }
          else
          {
            if (minFrontier.frontier.value > frontier.frontier.value)
            {
              minStageFrontier[frontier.node.DenseStageId] = frontier;
            }
          }
        }
      }

      Dictionary<int, List<Checkpoint>> checkpointsPerVertex =
        new Dictionary<int, List<Checkpoint>>();
      foreach (Checkpoint checkpoint in checkpoints)
      {
        if (checkpointsPerVertex.ContainsKey(checkpoint.node.denseId))
        {
          checkpointsPerVertex[checkpoint.node.denseId].Add(checkpoint);
        }
        else
        {
          List<Checkpoint> chks = new List<Checkpoint>();
          chks.Add(checkpoint);
          checkpointsPerVertex.Add(checkpoint.node.denseId, chks);
        }
      }

      Dictionary<int, List<Notification>> notifsPerVertex =
        new Dictionary<int, List<Notification>>();
      foreach (Notification notif in notifications)
      {
        if (notifsPerVertex.ContainsKey(notif.node.denseId))
        {
          notifsPerVertex[notif.node.denseId].Add(notif);
        }
        else
        {
          List<Notification> notifs = new List<Notification>();
          notifs.Add(notif);
          notifsPerVertex.Add(notif.node.denseId, notifs);
        }
      }

      Dictionary<int, List<DiscardedMessage>> discMsgPerStage =
        new Dictionary<int, List<DiscardedMessage>>();
      for (int i = 0; i < stageLenghts.Count; ++i)
      {
        discMsgPerStage.Add(i, new List<DiscardedMessage>());
      }
      foreach (DiscardedMessage discMsg in discMsgs)
      {
        discMsgPerStage[discMsg.dstDenseStage].Add(discMsg);
      }

      Dictionary<int, List<Edge>> edgesPerVertex =
        new Dictionary<int, List<Edge>>();
      foreach (Edge edge in graph)
      {
        if (edgesPerVertex.ContainsKey(edge.src.denseId))
        {
          edgesPerVertex[edge.src.denseId].Add(edge);
        }
        else
        {
          List<Edge> edges = new List<Edge>();
          edges.Add(edge);
          edgesPerVertex.Add(edge.src.denseId, edges);
        }
      }

      Dictionary<int, List<LexStamp>> delivDstTimePerEdgeKey =
        new Dictionary<int, List<LexStamp>>();

      foreach (DeliveredMessage delivMsg in delivMsgs)
      {
        var edgeKey = EdgeKey(delivMsg.srcDenseStage.PairWith(delivMsg.dst));
        if (delivDstTimePerEdgeKey.ContainsKey(edgeKey))
        {
          delivDstTimePerEdgeKey[edgeKey].Add(delivMsg.dstTime);
        }
        else
        {
          List<LexStamp> dstTimes = new List<LexStamp>();
          dstTimes.Add(delivMsg.dstTime);
          delivDstTimePerEdgeKey.Add(edgeKey, dstTimes);
        }
      }

      while (toProcess.Count > 0)
      {
        Frontier curFrontier = toProcess.Pop();
        List<Frontier> allNewFrontiers = new List<Frontier>();
        if (curFrontier.isNotification)
        {
          if (currentNFrontiers[curFrontier.node].frontier.Contains(curFrontier.frontier))
          {
            allNewFrontiers = ReduceMicroNotif(curFrontier, edgesPerVertex, notifsPerVertex, checkpointsPerVertex)
              .GroupBy(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)),
                       x => x,
                       (did, fs) => {
                         Frontier minF = fs.First();
                         foreach (var fff in fs)
                           {
                             if (fff.frontier.value < minF.frontier.value)
                               {
                                 minF = fff;
                               }
                           }
                         return minF;
                       }).ToList();
          }
        }
        else
        {
          if (currentFrontiers[curFrontier.node].frontier.Contains(curFrontier.frontier))
          {
            List<Frontier> reducedDiscards = new List<Frontier>();
            if (minStageFrontier[curFrontier.node.DenseStageId].frontier.value > curFrontier.frontier.value ||
                (minStageFrontier[curFrontier.node.DenseStageId].frontier.value == curFrontier.frontier.value &&
                 doneMinStageFrontier[curFrontier.node.DenseStageId] == false)) {
              minStageFrontier[curFrontier.node.DenseStageId] = curFrontier;
              doneMinStageFrontier[curFrontier.node.DenseStageId] = true;
              reducedDiscards =
                ReduceMicroForDiscarded(curFrontier, discMsgPerStage, checkpointsPerVertex);

            }
            List<Frontier> reduced =
              ReduceMicroDeliv(curFrontier, edgesPerVertex, delivDstTimePerEdgeKey, checkpointsPerVertex);
            allNewFrontiers = reduced.Concat(reducedDiscards)
              .GroupBy(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)),
                       x => x,
                       (did, fs) => {
                         Frontier minF = fs.First();
                         foreach (var fff in fs)
                           {
                             if (fff.frontier.value < minF.frontier.value)
                               {
                                 minF = fff;
                               }
                           }
                         return minF;
                       }).ToList();
          }
        }

        foreach (Frontier frontier in allNewFrontiers)
        {
          if (frontier.isNotification)
          {
            if (!frontier.frontier.Contains(currentNFrontiers[frontier.node].frontier))
            {
              currentNFrontiers[frontier.node] = frontier;
              toProcess.Push(frontier);
            }
          }
          else
          {
            if (!frontier.frontier.Contains(currentFrontiers[frontier.node].frontier))
            {
              currentFrontiers[frontier.node] = frontier;
              toProcess.Push(frontier);
              if (minStageFrontier[frontier.node.DenseStageId].frontier.value >
                  frontier.frontier.value) {
                minStageFrontier[frontier.node.DenseStageId] = frontier;
                doneMinStageFrontier[frontier.node.DenseStageId] = false;
              }
            }
          }
        }

      }
      return currentFrontiers.Values.Concat(currentNFrontiers.Values).ToList();
    }

    public List<Frontier> ComputeFrontiersScratch(
        System.Diagnostics.Stopwatch stopwatch,
        List<Edge> graph,
        List<Checkpoint> checkpoints,
        List<Notification> notifications,
        List<DeliveredMessage> delivMsgs,
        List<DiscardedMessage> discMsgs)
    {
      var frontiers = MaxFrontierPerVertex(checkpoints);
      Dictionary<SV, Frontier> currentFrontiers = new Dictionary<SV, Frontier>();
      Dictionary<SV, Frontier> currentNFrontiers = new Dictionary<SV, Frontier>();
      foreach (Frontier frontier in frontiers)
      {
        if (frontier.isNotification)
        {
          currentNFrontiers.Add(frontier.node, frontier);
        }
        else
        {
          currentFrontiers.Add(frontier.node, frontier);
        }
      }
      int numIterations = 0;
      while (true)
      {
        var reducedDiscards = ReduceForDiscardedScratch(frontiers, checkpoints, discMsgs);
        Console.WriteLine("ReduceForDiscardedScratch: {0}", stopwatch.ElapsedMilliseconds);
        var reduced = ReduceScratch(stopwatch, frontiers, checkpoints, delivMsgs, notifications, graph);
        Console.WriteLine("ReduceScratch: {0}", stopwatch.ElapsedMilliseconds);
        var allFrontiers = reduced.Concat(reducedDiscards).Concat(frontiers);
        //.Min(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)), ff => ff.frontier.value);
        frontiers = allFrontiers.GroupBy(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)),
                                         x => x,
                                         (did, fs) => {
                                           Frontier minF = fs.First();
                                           foreach (var fff in fs)
                                           {
                                             if (fff.frontier.value < minF.frontier.value)
                                             {
                                               minF = fff;
                                             }
                                           }
                                           return minF;
                                         }).ToList();

        List<Frontier> newFrontiers = new List<Frontier>();
        int numNewFrontiers = 0;
        foreach (Frontier frontier in frontiers)
        {
          if (frontier.isNotification)
          {
            if (!frontier.frontier.Contains(currentNFrontiers[frontier.node].frontier))
            {
              currentNFrontiers[frontier.node] = frontier;
              newFrontiers.Add(frontier);
              numNewFrontiers++;
            }
          }
          else
          {
            if (!frontier.frontier.Contains(currentFrontiers[frontier.node].frontier))
            {
              currentFrontiers[frontier.node] = frontier;
              newFrontiers.Add(frontier);
              numNewFrontiers++;
            }
          }
        }
        numIterations++;
        Console.WriteLine("Frontier equality: {0}", stopwatch.ElapsedMilliseconds);
        if (numNewFrontiers == 0)
        {
          Console.Error.WriteLine("Done num iterations {0} {1}", numIterations, stopwatch.ElapsedMilliseconds
);
          break;
        }
      }
      return currentFrontiers.Values.Concat(currentNFrontiers.Values).ToList();
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
      bool microBatch = false;
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
          case "-microbatch":
            microBatch = true;
            argIndex++;
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

      SerializationFormat serFormat =
        SerializationFactory.GetCodeGeneratorForVersion(this.config.SerializerVersion.First,
                                                        this.config.SerializerVersion.Second);

      List<Edge> edges = new List<Edge>();

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
              for (int i = 0; i < count; i++)
              {
                Edge edge = new Edge();
                edge.Restore2(onNextGraphReader);
                edges.Add(edge);
              }
              //                graphInput.OnNext(edges);
            }
          }
        }
      }
//        graphInput.OnCompleted();

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
            var initialStopwatch = System.Diagnostics.Stopwatch.StartNew();

            if (microBatch)
            {
              ComputeFrontiersMicroBatch(initialStopwatch,
                                         edges,
                                         initCheckpointChanges,
                                         initNotificationChanges,
                                         initDeliveredMessageChanges,
                                         initDiscardedMessageChanges);
            }
            else
            {
              ComputeFrontiersScratch(initialStopwatch,
                                      edges,
                                      initCheckpointChanges,
                                      initNotificationChanges,
                                      initDeliveredMessageChanges,
                                      initDiscardedMessageChanges);
            }
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
                List<Frontier> frontiers = new List<Frontier>();

                if (microBatch)
                {
                  frontiers = ComputeFrontiersMicroBatch(stopwatch,
                                                         edges,
                                                         checkpointState.ToList(),
                                                         notificationState.ToList(),
                                                         delivMsgState.ToList(),
                                                         discMsgState.ToList());
                }
                else
                {
                  frontiers = ComputeFrontiersScratch(stopwatch,
                                                      edges,
                                                      checkpointState.ToList(),
                                                      notificationState.ToList(),
                                                      delivMsgState.ToList(),
                                                      discMsgState.ToList());
                }

                Console.Error.WriteLine("Time to process epoch {0}: {1} {2} {3} {4} {5}", curEpoch, stopwatch.ElapsedMilliseconds, checkpointChanges.Count, notificationChanges.Count, deliveredMessageChanges.Count, discardedMessageChanges.Count);
                curEpoch++;
                if (curEpoch == replayNumEpochs)
                {
                  foreach (Frontier frontier in frontiers)
                  {
//                    Console.WriteLine("Frontier: {0}", frontier);
                  }
                }
              } else
              {
                break;
              }
            }
          }
        }

      }
    }
  }
}
