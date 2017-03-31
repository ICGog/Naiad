/*
 * Naiad ver. 0.6
 * Copyright (c) Microsoft Corporation
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
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;

namespace FaultToleranceExamples.ConnectedComponents
{
    public static class ExtensionMethods
    {

        public static Collection<IntPair, T> ConnectedComponents<T>(this Collection<IntPair, T> edges)
            where T : Time<T>
        {
            return edges.ConnectedComponents(x => 0);
        }

        /// <summary>
        /// the node names are now introduced in waves. propagation of each wave completes before the next starts.
        /// </summary>
        public static Collection<IntPair, T> ConnectedComponents<T>(this Collection<IntPair, T> edges, Func<IntPair, int> priorityFunction)
            where T : Time<T>
        {
            // initial labels only needed for min, as the max will be improved on anyhow.
            var nodes = edges.Select(x => new IntPair(Math.Min(x.s, x.t), Math.Min(x.s, x.t)))
                             .Consolidate();

            // symmetrize the graph
            edges = edges.Select(edge => new IntPair(edge.t, edge.s))
                         .Concat(edges);

            // prioritization introduces labels from small to large (in batches).
            var cc = nodes.Where(x => false)
                        .GeneralFixedPoint((lc, x) => x.Join(edges.EnterLoop(lc), n => n.s, e => e.s, (n, e) => new IntPair(e.t, n.t))
                                                             .Concat(nodes.EnterLoop(lc, priorityFunction))
                                                             .Min(n => n.s, n => n.t),
                                            priorityFunction,
                                            n => n.s,
                                           Int32.MaxValue, CheckpointType.Stateless);
            return cc;
        }
    }

    /// <summary>
    /// Demonstrates a connected components computation using Naiad's FixedPoint operator.
    /// </summary>
    public class ConnectedComponents : Example
    {
        int nodeCount = 5000000;
        int edgeCount =  3750000;
        // int nodeCount = 50000;
        // int edgeCount =  37500;

        private class FileLogStream : LogStream
        {
            private StreamWriter log;
            private FileStream logFile;
            public StreamWriter Log
            {
                get { return log; }
            }

            public void Flush()
            {
                lock (log)
                {
                    log.Flush();
                    logFile.Flush(true);
                }
            }

            private void FlushFileThread()
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    this.Flush();
                }
            }

            public FileLogStream(string prefix, string fileName)
            {
                this.logFile = new FileStream(Path.Combine(prefix, fileName), FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                this.log = new StreamWriter(this.logFile);
                var flush = new System.Threading.Thread(
                    new System.Threading.ThreadStart(() => this.FlushFileThread()));
                flush.Start();
            }
        }

        Configuration config;

        public void Execute(string[] args)
        {
            this.config = Configuration.FromArgs(ref args);
            this.config.MaxLatticeInternStaleTimes = 10;
            string logPrefix = "/tmp/falkirk/";
            bool minimalLogging = true;
            int managerWorkerCount = 4;
            bool nonIncrementalFTManager = false;
            int changesPerEpoch = 3;
            int numEpochsToRun = 10;

            System.IO.Directory.CreateDirectory(logPrefix);
            this.config.LogStreamFactory = (s => new FileLogStream(logPrefix, s));
            this.config.DefaultCheckpointInterval = 1000;
            System.IO.Directory.CreateDirectory(Path.Combine(logPrefix, "checkpoint"));
            this.config.CheckpointingFactory = s => new FileStreamSequence(Path.Combine(logPrefix, "checkpoint"), s);

            FTManager manager = new FTManager(this.config.LogStreamFactory,
                                              null,
                                              null,
                                              !nonIncrementalFTManager);
            using (var computation = NewComputation.FromConfig(this.config))
            {
                // establish numbers of nodes and edges from input or from defaults.
                if (args.Length == 4)
                {
                    nodeCount = Convert.ToInt32(args[1]);
                    edgeCount = Convert.ToInt32(args[2]);
                    changesPerEpoch = Convert.ToInt32(args[3]);
                }

                // generate a random graph
                var random = new Random(0);
                var graph = new IntPair[edgeCount];
                for (int i = 0; i < edgeCount; i++)
                    graph[i] = new IntPair(random.Next(nodeCount), random.Next(nodeCount));

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                // set up the CC computation
                var edges = computation.NewInputCollection<IntPair>();

                //Func<IntPair, int> priorityFunction = node => 0;
                //Func<IntPair, int> priorityFunction = node => Math.Min(node.t, 100);
                Func<IntPair, int> priorityFunction = node => 65536 * (node.t < 10 ? node.t : 10 + Convert.ToInt32(Math.Log(1 + node.t) / Math.Log(2.0)));

                using (var cp = computation.WithCheckpointPolicy(v => new CheckpointAtBatch<BatchIn<Epoch>>(2)))
                {

                var cc = edges.ConnectedComponents(priorityFunction)
                                  .Count(n => n.t, (l, c) => c)  // counts results with each label
                  .Consolidate();
                var output = cc.Subscribe(l =>
                    {
                      Console.Error.WriteLine("Time to process: {0}", stopwatch.Elapsed);
                      foreach (var result in l.OrderBy(x => x.record))
                        Console.Error.WriteLine(result);
                    });


                Console.Error.WriteLine("Connected components on a random graph ({0} nodes, {1} edges)",
                                        nodeCount, edgeCount);

                if (computation.Configuration.ProcessID == 0)
                {
                  manager.Initialize(computation,
                                     new int[] {cc.Output.ForStage.StageId},
                                     managerWorkerCount,
                                     minimalLogging);
                }

                computation.Activate();

                edges.OnNext(computation.Configuration.ProcessID == 0 ? graph : Enumerable.Empty<IntPair>());

                if (computation.Configuration.ProcessID == 0)
                {
                    output.Sync(0);
                    Console.WriteLine("Time post first sync {0}", stopwatch.ElapsedMilliseconds);
                    stopwatch.Restart();
                    int j = 0;
                    int i = 0;
                    for (; i < numEpochsToRun; i++)
                    {
                        List<Weighted<IntPair>> changes = new List<Weighted<IntPair>>();
                        for (int k = 0; k < changesPerEpoch; ++k, ++j)
                        {
                          changes.Add(new Weighted<IntPair>(graph[j], -1));
                          var newEdge = new IntPair(random.Next(nodeCount), random.Next(nodeCount));
                          changes.Add(new Weighted<IntPair>(newEdge, 1));
                        }
                        edges.OnNext(changes);
                    }
                    output.Sync(i);
                    Console.WriteLine("Total time {0}", stopwatch.ElapsedMilliseconds);
                }
                }
                edges.OnCompleted();
                computation.Join();
            }

        }

        public string Usage { get { return "[nodecount edgecount]"; } }

        public string Help
        {
            get { return ""; }
        }
    }
}
