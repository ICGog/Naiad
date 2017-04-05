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

namespace Microsoft.Research.Naiad.FaultToleranceManager
{
  public class UpdateFrontiers
  {
    private List<Pair<Frontier, bool>> toUpdate;
    private ManualResetEvent doneEvent;
    private FTManager manager;
    private List<Checkpoint> checkpoints;
    private List<DiscardedMessage> discMsgs;
    private List<DeliveredMessage> delivMsgs;
    private List<Notification> notifs;
    private List<Edge> graph;
    public List<Frontier> newFrontiers;

    public UpdateFrontiers(List<Pair<Frontier, bool>> curToUpdate,
                           ManualResetEvent doneEvent,
                           FTManager manager,
                           List<Checkpoint> checkpoints,
                           List<DiscardedMessage> discMsgs,
                           List<DeliveredMessage> delivMsgs,
                           List<Notification> notifs,
                           List<Edge> graph) {
      toUpdate = curToUpdate;
      this.doneEvent = doneEvent;
      this.manager = manager;
      this.checkpoints = checkpoints;
      this.discMsgs = discMsgs;
      this.delivMsgs = delivMsgs;
      this.notifs = notifs;
      this.graph = graph;
      this.newFrontiers = new List<Frontier>();
    }

    public void ThreadPoolCallback(Object threadContext)
    {
      System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
      var frontiers = toUpdate.Select(x => x.First).ToList();
      var reducedDiscards = ReduceForDiscardedScratch(frontiers).ToList();
      var reduced = ReduceScratch(frontiers, stopwatch);
      var allFrontiers = reduced.Concat(reducedDiscards).Concat(frontiers);
      newFrontiers = allFrontiers.GroupBy(ff => (ff.node.denseId + (ff.isNotification ? 0x10000 : 0)),
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
//      Console.WriteLine("{0} Thread took {1}", DateTime.UtcNow.ToString("HH:mm:ss.fff"), stopwatch.ElapsedMilliseconds);
      doneEvent.Set();
    }


    private List<Frontier> ReduceForDiscardedScratch(List<Frontier> frontiers)
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
              (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second, this.manager))
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

    private List<Frontier> ReduceScratch(List<Frontier> frontiers,
                                         System.Diagnostics.Stopwatch stopwatch)
    {
      List<Pair<Pair<int, SV>, LexStamp>> projectedMessageFrontiers = frontiers
        .Where(f => !f.isNotification)
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => e.src.DenseStageId
            .PairWith(e.dst)
            .PairWith(f.frontier.Project(
               manager.DenseStages[e.src.DenseStageId],
               manager.DenseStages[e.src.DenseStageId].DefaultVersion.Timestamp.Length))).ToList()
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

      List<Pair<SV,LexStamp>> staleDeliveredMessages = delivMsgs
        .Join(
          projectedMessageFrontiers, m => EdgeKey(m.srcDenseStage.PairWith(m.dst)), f => EdgeKey(f.First),
                (m, f) => f.First.Second.PairWith(m.dstTime.PairWith(f.Second)))
        .Where(m => !m.Second.Second.Contains(m.Second.First))
        .Select(m => m.First.PairWith(m.Second.First)).ToList();

      List<Frontier> intersectedProjectedNotificationFrontiers = frontiers
        .Where(f => f.isNotification)
        .Join(
          graph, f => f.node.denseId, e => e.src.denseId,
          (f, e) => new Frontier(e.dst, f.frontier.Project(
            manager.DenseStages[e.src.DenseStageId],
            manager.DenseStages[e.src.DenseStageId].DefaultVersion.Timestamp.Length), true))
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

      List<Pair<SV,LexStamp>> staleDeliveredNotifications = notifs
        .Join(
          intersectedProjectedNotificationFrontiers, n => n.node.denseId, f => f.node.denseId,
          (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
        .Where(n => !n.Second.Second.Contains(n.Second.First))
        .Select(n => n.First.PairWith(n.Second.First)).ToList();

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

      var reducedFrontiers = checkpoints
        .Join(
          earliestStaleEvents, c => c.node.denseId, e => e.First.denseId,
          (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second, manager))
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

      return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers).ToList();
    }

    private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(
      Checkpoint checkpoint,
      LexStamp time,
      FTManager manager) {

      if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(time))
      {
        return new Checkpoint(
          checkpoint.node,
          LexStamp.SetBelow(time, manager.DenseStages[checkpoint.node.DenseStageId].DefaultVersion.Timestamp.Length),
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
  }
}