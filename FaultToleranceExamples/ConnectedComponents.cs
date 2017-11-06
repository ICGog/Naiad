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
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Diagnostics;

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
        public static Collection<IntPair, T> ConnectedComponents<T>(this Collection<IntPair, T> edges,
                                                                    Func<IntPair, int> priorityFunction)
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
//            this.config.MaxLatticeInternStaleTimes = 100;
            this.config.DefaultCheckpointInterval = 60000;
            bool syncEachEpoch = false;
            bool checkpointEagerly = false;
            string logPrefix = "/mnt/ramdisk/falkirk/";
            bool minimalLogging = false;
            int managerWorkerCount = 4;
            bool nonIncrementalFTManager = false;
            int checkpointTimeLength = 1;
            int changesPerEpoch = 30;
            int numEpochsToRun = 50;
            // int nodeCount = 5000000;
            // int edgeCount =  3750000;
            int nodeCount = 50000;
            int edgeCount =  37500;
            bool noFaultTolerance = false;
            int i = 1;
            while (i < args.Length)
            {
                switch (args[i].ToLower())
                {
                    case "-minimallog":
                        minimalLogging = true;
                        ++i;
                        break;
                    case "-nonincrementalftmanager":
                        nonIncrementalFTManager = true;
                        i++;
                        break;
                    case "-mwc":
                        managerWorkerCount = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "-logprefix":
                        logPrefix = args[i + 1];
                        i += 2;
                        break;
                    case "-changesperepoch":
                        changesPerEpoch = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "-numepochstorun":
                        numEpochsToRun = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "-nodecount":
                        nodeCount = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "-edgecount":
                        edgeCount = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "-nofaulttolerance":
                        noFaultTolerance = true;
                        i++;
                        break;
                    case "-synceachepoch":
                        syncEachEpoch = true;
                        i++;
                        break;
                    case "-checkpointeagerly":
                        checkpointEagerly = true;
                        i++;
                        break;
                    case "-checkpointtimelength":
                        checkpointTimeLength = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    default:
                        throw new ApplicationException("Unknown argument " + args[i]);
                }
            }

            System.IO.Directory.CreateDirectory(logPrefix);
            this.config.LogStreamFactory = (s => new FileLogStream(logPrefix, s));
            if (!noFaultTolerance)
            {
              System.IO.Directory.CreateDirectory(Path.Combine(logPrefix, "checkpoint"));
              this.config.CheckpointingFactory = s => new FileStreamSequence(Path.Combine(logPrefix, "checkpoint"), s);
            }
            SerializationFormat serFormat =
              SerializationFactory.GetCodeGeneratorForVersion(this.config.SerializerVersion.First,
                                                              this.config.SerializerVersion.Second);
            FileStream onNextStream = File.Create(logPrefix + "/onNext.log");
            FileStream onNextGraphStream = File.Create(logPrefix + "/onNextGraph.log");
            NaiadWriter onNextWriter = new NaiadWriter(onNextStream, serFormat);
            NaiadWriter onNextGraphWriter = new NaiadWriter(onNextGraphStream, serFormat);

            FTManager manager = new FTManager(this.config.LogStreamFactory,
                                              //onNextWriter,
                                              //onNextGraphWriter,
                                              null, null,
                                              !nonIncrementalFTManager);
            using (var computation = NewComputation.FromConfig(this.config))
            {
                if (!checkpointEagerly)
                {
                  computation.WithCheckpointPolicy(v => new CheckpointAtBatch<BatchIn<Epoch>>(checkpointTimeLength));
                } else
                {
                  computation.WithCheckpointPolicy(v => new CheckpointEagerly());
                }
                // generate a random graph
                var random = new Random(0);
                var graph = new IntPair[edgeCount];
                for (int index = 0; index < edgeCount; index++)
                    graph[index] = new IntPair(random.Next(nodeCount), random.Next(nodeCount));

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                // set up the CC computation
                var edges = computation.NewInputCollection<IntPair>();
                if (!checkpointEagerly)
                {
                  edges.SetCheckpointType(CheckpointType.Stateless);
                }
                else
                {
                  edges.SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(s => new CheckpointEagerly());
                }

                //Func<IntPair, int> priorityFunction = node => 0;
                //Func<IntPair, int> priorityFunction = node => Math.Min(node.t, 100);
                Func<IntPair, int> priorityFunction = node => 65536 * (node.t < 10 ? node.t : 10 + Convert.ToInt32(Math.Log(1 + node.t) / Math.Log(2.0)));

                var cc = edges.ConnectedComponents(priorityFunction)
                  .Count(n => n.t, (l, c) => c);
//                  .Consolidate();

                if (!checkpointEagerly)
                {
                  cc.SetCheckpointType(CheckpointType.Stateless);
                }
                else
                {
                  cc.SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(s => new CheckpointEagerly());
                }

                // var output = cc.Subscribe(stopwatch, l =>
                //     {
                //       Console.Error.WriteLine("Time to process: {0}", stopwatch.Elapsed);
                //       foreach (var result in l.OrderBy(x => x.record))
                //         Console.Error.WriteLine(result);
                //     });
                var output = cc.Subscribe(l => { });


                Console.Error.WriteLine("Connected components on a random graph ({0} nodes, {1} edges)",
                                        nodeCount, edgeCount);

                if (computation.Configuration.ProcessID == 0 && !noFaultTolerance)
                {
                  manager.Initialize(computation,
                                     new int[] {cc.Output.ForStage.StageId},
                                     managerWorkerCount,
                                     minimalLogging);

                }

                computation.Activate();
                Console.WriteLine("Initialized {0}", stopwatch.ElapsedMilliseconds);
//                edges.OnNext(computation.Configuration.ProcessID == 0 ? graph : Enumerable.Empty<IntPair>());
                edges.OnNext(graph);

//                if (computation.Configuration.ProcessID == 0)
                {
                    output.Sync(0);
                    Console.WriteLine("Time post first sync {0}", stopwatch.ElapsedMilliseconds);
                    stopwatch.Restart();
                    int j = 0;
                    int curEpoch = 1;
                    for (; curEpoch <= numEpochsToRun; curEpoch++)
                    {
                        List<Weighted<IntPair>> changes = new List<Weighted<IntPair>>();
                        for (int k = 0; k < changesPerEpoch; ++k, ++j)
                        {
                          j = j % edgeCount;
                          changes.Add(new Weighted<IntPair>(graph[j], -1));
                          var newEdge = new IntPair(random.Next(nodeCount), random.Next(nodeCount));
                          changes.Add(new Weighted<IntPair>(newEdge, 1));
                        }
                        if (syncEachEpoch)
                        {
                          output.Sync(curEpoch - 1);
                        }
                        edges.OnNext(changes);
                    }
                    output.Sync(numEpochsToRun);
                    Console.WriteLine("Total time {0}", stopwatch.ElapsedMilliseconds);
                }
                edges.OnCompleted();
                computation.Join();
                if (computation.Configuration.ProcessID == 0 && !noFaultTolerance)
                {
                  manager.Join();
                }
            }

        }

        public string Usage { get { return "[nodecount edgecount]"; } }

        public string Help
        {
            get { return ""; }
        }
    }
}
