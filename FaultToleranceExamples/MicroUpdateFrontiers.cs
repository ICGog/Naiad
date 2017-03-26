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
//using FaultToleranceExamples.ReplayIncrementalComplexFTWorkflow;

namespace FaultToleranceExamples
{

  public class UpdateFrontiers
  {
    private List<Pair<Frontier, bool>> toUpdate;
    private ManualResetEvent doneEvent;
    private ReplayIncrementalComplexFTWorkflow.ReplayIncrementalComplexFTWorkflow replay;
    private Dictionary<int, List<Edge>> edgesPerVertex;
    private Dictionary<int, List<Notification>> notifsPerVertex;
    private Dictionary<int, List<Checkpoint>> checkpointsPerVertex;
    private Dictionary<int, List<DiscardedMessage>> discMsgPerStage;
    private Dictionary<int, List<LexStamp>> delivDstTimePerEdgeKey;
    public List<Frontier> newFrontiers;

    public UpdateFrontiers(List<Pair<Frontier, bool>> curToUpdate,
                           ManualResetEvent doneEvent,
                           ReplayIncrementalComplexFTWorkflow.ReplayIncrementalComplexFTWorkflow replay,
                           Dictionary<int, List<Edge>> edgesPerVertex,
                           Dictionary<int, List<Notification>> notifsPerVertex,
                           Dictionary<int, List<Checkpoint>> checkpointsPerVertex,
                           Dictionary<int, List<DiscardedMessage>> discMsgPerStage,
                           Dictionary<int, List<LexStamp>> delivDstTimePerEdgeKey)
    {
      toUpdate = curToUpdate;
      this.doneEvent = doneEvent;
      this.replay = replay;
      this.edgesPerVertex = edgesPerVertex;
      this.notifsPerVertex = notifsPerVertex;
      this.checkpointsPerVertex = checkpointsPerVertex;
      this.discMsgPerStage = discMsgPerStage;
      this.delivDstTimePerEdgeKey = delivDstTimePerEdgeKey;
      this.newFrontiers = new List<Frontier>();
    }

    public void ThreadPoolCallback(Object threadContext)
    {
      foreach (var frontierDiscard in toUpdate)
      {
        if (frontierDiscard.First.isNotification)
        {
          newFrontiers = newFrontiers.Concat(UpdateNotification(frontierDiscard.First)).ToList();
        }
        else
        {
          newFrontiers = newFrontiers.Concat(UpdateMsgs(frontierDiscard.First, frontierDiscard.Second)).ToList();
        }
      }
      doneEvent.Set();
    }

    public List<Frontier> UpdateMsgs(Frontier frontier, bool discarded)
    {
      List<Frontier> reducedDiscards = new List<Frontier>();
      if (discarded)
      {
        reducedDiscards = ReduceMicroForDiscarded(frontier,
                                                  discMsgPerStage,
                                                  checkpointsPerVertex);
      }
      List<Frontier> reduced = ReduceMicroDeliv(frontier,
                                                edgesPerVertex,
                                                delivDstTimePerEdgeKey,
                                                checkpointsPerVertex,
                                                replay);
      return reduced.Concat(reducedDiscards)
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

    public List<Frontier> UpdateNotification(Frontier frontier)
    {
      return ReduceMicroNotif(frontier,
                              edgesPerVertex,
                              notifsPerVertex,
                              checkpointsPerVertex,
                              replay)
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

    public List<Frontier> ReduceMicroDeliv(Frontier curFrontier,
                                           Dictionary<int, List<Edge>> edgesPerVertex,
                                           Dictionary<int, List<LexStamp>> delivDstTimePerEdgeKey,
                                           Dictionary<int, List<Checkpoint>> checkpointsPerVertex,
                                           ReplayIncrementalComplexFTWorkflow.ReplayIncrementalComplexFTWorkflow replay)
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
                            replay.StageTypes[edge.src.DenseStageId],
                            replay.StageLenghts[edge.src.DenseStageId])))
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
            var c = PairCheckpointToBeLowerThanTime(checkpoint, e.Second, replay);
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
            var c = PairCheckpointToBeLowerThanTime(checkpoint, minC.Second, replay);
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
                                           Dictionary<int, List<Checkpoint>> checkpointsPerVertex,
                                           ReplayIncrementalComplexFTWorkflow.ReplayIncrementalComplexFTWorkflow replay)
    {
      List<Edge> vEdges = new List<Edge>();
      bool found = edgesPerVertex.TryGetValue(curFrontier.node.denseId, out vEdges);
      List<Frontier> intersectedProjectedNotificationFrontiers = new List<Frontier>();
      if (found)
      {
        intersectedProjectedNotificationFrontiers =
        vEdges.Select(e => new Frontier(e.dst, curFrontier.frontier.Project(
            replay.StageTypes[e.src.DenseStageId],
            replay.StageLenghts[e.src.DenseStageId]), true))
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
            var c = PairCheckpointToBeLowerThanTime(checkpoint, e.Second, replay);
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

    private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(
      Checkpoint checkpoint,
      LexStamp time,
      ReplayIncrementalComplexFTWorkflow.ReplayIncrementalComplexFTWorkflow replayWorkflow) {

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
  }
}