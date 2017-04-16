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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Frameworks;
using System.IO;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Net;
using Microsoft.Research.Naiad.Utilities;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Networking;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime;

using System.Diagnostics;
using Microsoft.Research.Naiad.Dataflow;
//using System.Net.NetworkInformation;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Manages the execution of Naiad programs in a single process.
    /// </summary>
    /// <remarks>
    /// A Naiad Controller manages the execution of one or more <see cref="Computation"/> 
    /// instances (or "computations"). To construct an instance of this interface, use the
    /// static methods of the <see cref="NewController"/> class.
    /// </remarks>
    /// <seealso cref="NewController"/>
    public interface Controller : IDisposable
    {
        /// <summary>
        /// The configuration used by this controller.
        /// </summary>
        Configuration Configuration { get; }

        /// <summary>
        /// Constructs a new computation in this controller.
        /// </summary>
        /// <returns>The dataflow graph manager for the new computation.</returns>
        /// <example>
        /// A computation is typically created in a <see cref="Controller"/> as follows:
        /// <code>
        /// using Microsoft.Research.Naiad;
        /// 
        /// class Program
        /// {
        ///     public static void Main(string[] args)
        ///     {
        ///         using (Controller controller = NewController.FromArgs(ref args))
        ///         {
        ///             using (Computation computation = controller.NewComputation())
        ///             {
        ///                 /* Computation goes here. */
        ///                 
        ///                 computation.Join();
        ///             }
        /// 
        ///             controller.Join();
        ///         }
        ///     }
        /// }
        /// </code>
        /// </example>
        /// <seealso cref="NewController.FromArgs"/>
        /// <seealso cref="Computation.Join"/>
        /// <see cref="Controller.Join"/>
        Computation NewComputation();
        
        #region Checkpoint / Restore

        //void Checkpoint(bool major);
        //void Checkpoint(string path, int epoch, int computationIndex);
        
        //void Restore(string path, int epoch, int computationIndex);
        //void Restore(NaiadReader reader);

        //void Pause();
        //void Resume();

        #endregion

        /// <summary>
        /// The workers associated with this controller.
        /// </summary>
        WorkerGroup WorkerGroup { get; }

        /// <summary>
        /// The default placement of new stages.
        /// </summary>
        Placement DefaultPlacement { get; }

        /// <summary>
        /// A stopwatch that is started when the computation starts
        /// </summary>
        Stopwatch Stopwatch { get; }

        /// <summary>
        /// Blocks the caller until all computation in this controller has terminated.
        /// </summary>
        /// <remarks>
        /// This method must be called before calling Dispose(),
        /// or an error will be raised.
        /// </remarks>
        /// <example>
        /// The typical usage of Join is before the end of the <c>using</c> block for a
        /// Controller:
        /// <code>
        /// using (Controller controller = NewController.FromArgs(ref args))
        /// {
        ///     /* Computations go here. */
        ///     
        ///     controller.Join();
        /// }
        /// </code>
        /// </example>
        /// <seealso cref="NewController.FromArgs"/>
        void Join();

        /// <summary>
        /// Returns a task that blocks until all computation in this controller has terminated.
        /// </summary>
        /// <returns>A task that blocks until all computation is complete</returns>
        Task JoinAsync();

        /// <summary>
        /// The serialization format used for all communication in this controller.
        /// </summary>
        SerializationFormat SerializationFormat { get; }
    }

    internal interface InternalController
    {
        Configuration Configuration { get; }

        InternalWorkerGroup Workers { get; }

        Stopwatch Stopwatch { get; }

        Placement DefaultPlacement { get; }

        Object GlobalLock { get; }

        SerializationFormat SerializationFormat { get; }

        NetworkChannel NetworkChannel { get; }

        void DoStartupBarrier();
        void TriggerSimulatedFailure(int processId, int restartDelay);
        void ReportSimulatedFailureRestart(int processId);
        void SimulateFailure(int delay);
        bool HasFailed { get; }
        long TicksSinceStartup { get; }

        void PauseWithoutRollback();
        void ResumeWithoutRollback();

        void PausePeerProcesses(IEnumerable<int> processes);
        void StartRollback(Action<string> logAction);

        void RestoreToFrontiers(int graphId, IEnumerable<CheckpointLowWatermark> frontiers, Action<string> logAction);

        void ResetProgress();

        void SignalPause();
        void SignalRestore();

        InternalComputation GetInternalComputation(int index);

        void WriteLog(string entry);

        void Checkpoint(string path, int epoch, int computationIndex);
        void Restore(string path, int epoch, int computationIndex);

        Controller ExternalController { get; }
    }

    /// <summary>
    /// Provides static constructors for creating a <see cref="OneOffComputation"/>.
    /// </summary>
    public static class NewComputation
    {
        /// <summary>
        /// Constructs a <see cref="OneOffComputation"/> with a configuration extracted from the given command-line arguments.
        /// </summary>
        /// <param name="args">The command-line arguments, which will have Naiad-specific arguments removed.</param>
        /// <returns>A new <see cref="OneOffComputation"/> based on the given arguments.</returns>
        /// <remarks>
        /// This class provides a convenient mechanism for initializing a Naiad program that contains a single computation,
        /// by combining the roles of a <see cref="Controller"/> and a <see cref="Computation"/>.
        /// For more complicated cases, use <see cref="NewController.FromArgs"/> and <see cref="Controller.NewComputation"/>.
        /// </remarks>
        /// <example>
        /// Many Naiad programs initialize the <see cref="OneOffComputation"/> as follows:
        /// 
        /// using Microsoft.Research.Naiad;
        /// 
        /// class Program
        /// {
        ///     public static void Main(string[] args)
        ///     {
        ///         using (OneOffComputation computation = NewComputation.FromArgs(ref args))
        ///         {
        ///             /* Computation goes here. */
        ///                 
        ///             computation.Join();
        ///         }
        ///     }
        /// }
        /// </example>
        /// <seealso cref="Controller"/>
        /// <seealso cref="Computation"/>
        public static OneOffComputation FromArgs(ref string[] args)
        {
            return new InternalOneOffComputation(Configuration.FromArgs(ref args));
        }

        /// <summary>
        /// Constructs a <see cref="OneOffComputation"/> with the given configuration.
        /// </summary>
        /// <param name="conf">The configuration.</param>
        /// <returns>A new <see cref="OneOffComputation"/> based on the given arguments.</returns>
        public static OneOffComputation FromConfig(Configuration conf)
        {
            return new InternalOneOffComputation(conf);
        }
    }

    /// <summary>
    /// Provides static constructors for creating a <see cref="Controller"/>.
    /// </summary>
    public static class NewController
    {
        /// <summary>
        /// Constructs a <see cref="Controller"/> with a configuration extracted from the given command-line arguments.
        /// </summary>
        /// <param name="args">The command-line arguments, which will have Naiad-specific arguments removed.</param>
        /// <returns>A new <see cref="Controller"/> based on the given arguments.</returns>
        /// <example>
        /// Many Naiad programs initialize the <see cref="Controller"/> as follows:
        /// <code>
        /// using Microsoft.Research.Naiad;
        /// 
        /// class Program
        /// {
        ///     public static void Main(string[] args)
        ///     {
        ///         using (Controller controller = NewController.FromArgs(ref args))
        ///         {
        ///             using (Computation computation = controller.NewComputation())
        ///             {
        ///                 /* Computation goes here. */
        ///                 
        ///                 computation.Join();
        ///             }
        /// 
        ///             controller.Join();
        ///         }
        ///     }
        /// }
        /// </code>
        /// </example>
        /// <seealso cref="Controller.Join"/>
        /// <seealso cref="Controller.NewComputation"/>
        /// <seealso cref="Computation.Join"/>
        public static Controller FromArgs(ref string[] args)
        {
            return FromConfig(Configuration.FromArgs(ref args));
        }

        /// <summary>
        /// Constructs a <see cref="Controller"/> with the given <see cref="Configuration"/>.
        /// </summary>
        /// <param name="conf">The configuration</param>
        /// <returns>A new <see cref="Controller"/> with the given <see cref="Configuration"/>.</returns>
        public static Controller FromConfig(Configuration conf)
        {
            return new BaseController(conf);
        }
    }

    /// <summary>
    /// Responsible for managing the execution of multiple worker threads within a process.
    /// </summary>
    internal class BaseController : IDisposable, InternalController, Controller
    {
        private readonly List<BaseComputation> baseComputations;

        public Controller ExternalController { get { return this; } }

        public InternalComputation GetInternalComputation(int index)
        {
            return this.baseComputations[index];
        }

        internal string QueryStatistic(RuntimeStatistic stat)
        {
            var result = this.QueryStatisticAsLong(stat);
            if (result == null)
                return "-";
            else
                return Convert.ToString(result);
        }

        public event EventHandler OnStartup;

        protected void NotifyOnStartup()
        {
            if (this.OnStartup != null)
                this.OnStartup(this, new EventArgs());
        }

        public event EventHandler OnShutdown;

        protected void NotifyOnShutdown()
        {
            if (this.OnShutdown!= null)
                this.OnShutdown(this, new EventArgs());
        }

        private readonly Configuration configuration;
        public Configuration Configuration
        {
            get { return this.configuration; }
        }

        private readonly System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
        public System.Diagnostics.Stopwatch Stopwatch { get { return this.stopwatch; } }

        private readonly Object globalLock = new object();
        public Object GlobalLock { get { return this.globalLock; } }

        public SerializationFormat SerializationFormat { get; private set; }

        #region Checkpoint / Restore

        private AutoResetEvent restoreEvent;
        private volatile bool aborted = false;
        private Thread restoreThread;

        private AutoResetEvent pauseEvent;
        private Thread pauseThread;

        private StreamWriter checkpointLog = null;
        private StreamWriter CheckpointLog
        {
            get
            {
                if (checkpointLog == null)
                {
                    string fileName = String.Format("controller.{0:D3}.log",
                        this.Configuration.ProcessID);
                    checkpointLog = this.Configuration.LogStreamFactory(fileName).Log;
                }
                return checkpointLog;
            }
        }

        public void WriteLog(string entry)
        {
            var log = this.CheckpointLog;
            lock (log)
            {
                log.WriteLine(entry);
            }
        }

        private void ShowWaitingNotifications()
        {
            IEnumerable<Scheduler.WorkItem> items = new Scheduler.WorkItem[0];
            foreach (var scheduler in this.workerGroup.schedulers)
            {
                foreach (var computation in this.baseComputations)
                {
                    foreach (var stage in computation.Stages.OrderBy(s => s.Key))
                    {
                        foreach (var vertex in stage.Value.Vertices.OrderBy(v => v.VertexId))
                        {
                            items = items.Concat(scheduler.GetWorkItemsForVertex(vertex));
                        }
                    }
                }
            }

            foreach (var stage in items.GroupBy(i => i.Vertex.Stage.StageId))
            {
                foreach (var vertex in stage.GroupBy(v => v.Vertex.VertexId))
                {
                    Console.Write(stage.Key + "." + vertex.Key + ":");

                    foreach (var time in vertex.GroupBy(v => v.Requirement))
                    {
                        Console.Write(" " + time.Key + ":" + time.Count());
                    }

                    Console.WriteLine();
                }
            }
        }

        public void PauseThread()
        {
          while (true)
          {
            this.pauseEvent.WaitOne();
            if (this.aborted)
            {
              return;
            }

            this.Workers.PauseWithoutRollback();
            // if (this.networkChannel != null && this.networkChannel is Snapshottable)
            // {
            //     ((Snapshottable)this.networkChannel).AnnounceWorldStopped();
            // }
            // block until the computation is resumed.
            this.pauseEvent.WaitOne();
            this.Workers.ResumeWithoutRollback();
            // if (this.networkChannel != null && this.networkChannel is Snapshottable)
            // {
            //     ((Snapshottable)this.networkChannel).AnnounceWorldResumed();
            // }
          }
        }

        public void RestorationThread()
        {
            while (true)
            {
                this.restoreEvent.WaitOne();

                if (this.aborted)
                {
                    return;
                }

                long startTicks = this.stopwatch.ElapsedTicks;

                this.Pause(s => Console.WriteLine(s));

                long pauseTicks = this.stopwatch.ElapsedTicks;

                this.ResetProgress();

                foreach (BaseComputation computation in this.baseComputations)
                {
                    computation.ProgressTracker.Reset();
                }

                long progressTicks = this.stopwatch.ElapsedTicks;

                Console.WriteLine("Paused in preparation for restoration");

                // block until the coordinator has reported that the progress mechanism has been repaired
                this.restoreEvent.WaitOne();

                long repairTicks = this.stopwatch.ElapsedTicks;

                Console.WriteLine("Heard progress is restored");

                foreach (BaseComputation computation in this.baseComputations)
                {
                    computation.Rollback();
                }

                long rollbackTicks = this.stopwatch.ElapsedTicks;

                Console.WriteLine("Restarting receive threads");

                this.ResumeAfterRollback();

                long resumeTicks = this.stopwatch.ElapsedTicks;

                foreach (BaseComputation computation in this.baseComputations)
                {
                    computation.RestartAfterRollback();
                }

                long totalTicks = this.stopwatch.ElapsedTicks;

                long pauseMicroSeconds = ((pauseTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long progressMicroSeconds = ((progressTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long repairMicroSeconds = ((repairTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long rollbackMicroSeconds = ((rollbackTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long resumeMicroSeconds = ((resumeTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long restartMicroSeconds = ((totalTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                this.WriteLog(String.Format("{0:D3} P {1:D7} {2:D7} {3:D7} {4:D7} {5:D7} {6:D7} {7:D11}",
                    this.configuration.ProcessID,
                    pauseMicroSeconds, progressMicroSeconds, repairMicroSeconds,
                    rollbackMicroSeconds, resumeMicroSeconds, restartMicroSeconds,
                    totalMicroSeconds));

                Console.WriteLine("Finished rollback");
            }
        }

        public void PausePeerProcesses(IEnumerable<int> processes)
        {
            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                ((Snapshottable)this.networkChannel).StartRollback(processes);
                ((Snapshottable)this.networkChannel).WaitForWorkerPausedMessages(processes);
            }
        }

        public void StartRollback(Action<string> logAction)
        {
            logAction("preparing to Pause");
            this.Pause(logAction);
            logAction("paused");

            // forward any fault tolerance updates we received after pausing
            this.FlushFinalFaultToleranceTraffic();
            logAction("flushed");

            // stop sending any clock updates from the centralizer, in preparation for the surgery we will do below
            foreach (BaseComputation computation in this.baseComputations)
            {
                computation.ProgressTracker.PrepareForRollback(true);
            }
            logAction("prepared progress");

            foreach (BaseComputation computation in this.baseComputations)
            {
                //StringWriter w = new StringWriter();
                //computation.ProgressTracker.Complain(w);
                //Console.WriteLine(w);
                // clear out all progress information
                computation.ProgressTracker.Reset();
                // let the centralizer forward updates again for the next phase in which everyone sends
                // out their post-rollback progress items
                computation.ProgressTracker.PrepareForRollback(false);
            }
            logAction("reset progress");

            Console.WriteLine("Initiator paused in preparation for restoration");
        }

        public void RestoreToFrontiers(int computationIndex, IEnumerable<CheckpointLowWatermark> frontiers, Action<string> logAction)
        {
            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                ((Snapshottable)this.networkChannel).BroadcastCheckpoints(computationIndex, frontiers);
            }

            logAction("broadcast checkpoints");

            this.baseComputations[computationIndex].ReceiveCheckpointFrontiersAndRepairProgress(frontiers);

            logAction("received frontiers");

            Console.WriteLine("Waiting for progress");

            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                // wait for peers to send their progress traffic
                ((Snapshottable)this.networkChannel).WaitForAllRollbackProgressMessages();
                logAction("got progress traffic");
                // tell them all the progress traffic has arrived, so they can shut down their
                // receive thread and restore before we start sending messages again
                ((Snapshottable)this.networkChannel).SignalProgressRepaired(true);
            }

            logAction("rolling back");
            Console.WriteLine("Rolling back");

            this.baseComputations[computationIndex].Rollback();

            Console.WriteLine("Resuming");
            logAction("resuming");

            this.ResumeAfterRollback();

            Console.WriteLine("Restarting");
            logAction("restarting");

            this.baseComputations[computationIndex].RestartAfterRollback();

            Console.WriteLine("Finished rollback");
            logAction("finished rollback");
        }

        public void ResetProgress()
        {
            foreach (var scheduler in this.workerGroup.schedulers)
            {
                scheduler.ResetProgress();
            }
        }

        public void SignalRestore()
        {
            this.restoreEvent.Set();
        }

        public void SignalPause()
        {
            this.pauseEvent.Set();
        }

        public void Checkpoint(bool major)
        {
            throw new NotImplementedException();
        }

        public void Checkpoint(string path, int epoch, int computationIndex)
        {
            Stopwatch checkpointWatch = Stopwatch.StartNew();
            SerializationFormat serFormat =
              SerializationFactory.GetCodeGeneratorForVersion(this.configuration.SerializerVersion.First,
                                                              this.configuration.SerializerVersion.Second);

            foreach (Dataflow.InputStage input in this.baseComputations[computationIndex].Inputs)
            {
                using (FileStream collectionFile = File.OpenWrite(Path.Combine(path, string.Format("input_{0}_{1}.vertex", input.InputId, epoch))))
                  using (NaiadWriter collectionWriter = new NaiadWriter(collectionFile, serFormat))
                {
                    input.CheckpointFull(collectionWriter);
//                    Console.Error.WriteLine("Read  {0}: {1} objects", input.ToString(), collectionWriter.objectsWritten);
                }
            }
            foreach (Vertex vertex in this.baseComputations[computationIndex].Stages.Select(x => x.Value).SelectMany(x => x.Vertices.Where(s => s.Stateful)))
            {
//                vertex.Checkpoint(false);
                using (FileStream vertexFile = File.OpenWrite(Path.Combine(path, string.Format("{0}_{1}_{2}.vertex", vertex.Stage.StageId, vertex.VertexId, epoch))))
                  using (NaiadWriter vertexWriter = new NaiadWriter(vertexFile, serFormat))
                {
                    vertex.Checkpoint(vertexWriter);
//                    Console.Error.WriteLine("Wrote {0}: {1} objects", vertex.ToString(), vertexWriter.objectsWritten);
                }
            }

            List<Pair<int, Pointstamp>> stagePointstamp = new List<Pair<int, Pointstamp>>();

            foreach (Vertex vertex in this.baseComputations[computationIndex].Stages.Select(x => x.Value).SelectMany(x => x.Vertices))
            {
              stagePointstamp.Add(vertex.Stage.StageId.PairWith(vertex.lastCompletedStamp));
            }

            Console.Error.WriteLine("!! Total checkpoint took time = {0}", checkpointWatch.Elapsed);

            baseComputations[computationIndex].stagePointstamps.OnNext(stagePointstamp);

            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
              ((Snapshottable)this.networkChannel).AnnounceStopCheckpoint();
              ((Snapshottable)this.networkChannel).WaitForAllStopCheckpointMessages();
            }
        }

        public void Restore(string path, int epoch, int computationIndex)
        {
            Stopwatch checkpointWatch = Stopwatch.StartNew();

            SerializationFormat serFormat =
              SerializationFactory.GetCodeGeneratorForVersion(this.configuration.SerializerVersion.First,
                                                              this.configuration.SerializerVersion.Second);

            // Need to do this to ensure that all stages exist.
            this.baseComputations[computationIndex].MaterializeAll(false);

            foreach (var input in this.baseComputations[computationIndex].Inputs)
            {
                using (FileStream collectionFile = File.OpenRead(Path.Combine(path, string.Format("input_{0}_{1}.vertex", input.InputId, epoch))))
                using (NaiadReader collectionReader = new NaiadReader(collectionFile, serFormat))
                {
                    input.RestoreFull(collectionReader);
//                    Console.Error.WriteLine("Read  {0}: {1} objects", input.ToString(), collectionReader.objectsRead);
                }
            }
            foreach (var vertex in this.baseComputations[computationIndex].Stages.Select(x => x.Value).SelectMany(x => x.Vertices.Where(s => s.Stateful)))
            {
                using (FileStream vertexFile = File.OpenRead(Path.Combine(path, string.Format("{0}_{1}_{2}.vertex", vertex.Stage.StageId, vertex.VertexId, epoch))))
                using (NaiadReader vertexReader = new NaiadReader(vertexFile, serFormat))
                {
                    vertex.Restore(vertexReader);
//                    Console.Error.WriteLine("Read  {0}: {1} objects", vertex.ToString(), vertexReader.objectsRead);
                }
            }
            this.Workers.Activate();
            this.baseComputations[computationIndex].Activate();
            
            Console.Error.WriteLine("!! Total restore took time = {0}", checkpointWatch.Elapsed);
            Logging.Info("! Reactivated the controller");
        }

        public void Restore(NaiadReader reader)
        {
            throw new NotImplementedException();

#if false
            foreach (var kvp in this.currentGraphManager.Stages.OrderBy(x => x.Key))
            {
                int before = reader.objectsRead;
                kvp.Value.Restore(reader);
                int after = reader.objectsRead;
                Logging.Info("! Restored collection {0}, objects = {1}", kvp.Value, after - before);
            }
            this.Workers.Activate();
            this.currentGraphManager.Activate();
#endif
            //Logging.Info("! Reactivated the controller");
        }

        #endregion

 

        /// <summary>
        /// Represents a groupb of Naiad workers that are controlled by a single Controller.
        /// </summary>
        public class BaseWorkerGroup : InternalWorkerGroup
        {
            private readonly int numWorkers;
            /// <summary>
            /// Returns the number of workers in this group.
            /// </summary>
            public int Count { get { return this.numWorkers; } }

            // Optional support for broadcast scheduler wakeup
            internal bool useBroadcastWakeup;
            internal EventCount wakeUpEvent;

            private int totalSharedItemInCount = 0;
            private int totalSharedItemOutCount = 0;

            public int IncrementSharedQueueCount(int queuedItemCount)
            {
                lock (this)
                {
                    totalSharedItemInCount += queuedItemCount;
                    return totalSharedItemInCount;
                }
            }

            public int DecrementSharedQueueCount(int dequeuedItemCount)
            {
                lock (this)
                {
                    totalSharedItemOutCount += dequeuedItemCount;
                    if (totalSharedItemOutCount == totalSharedItemInCount)
                    {
                        return totalSharedItemOutCount;
                    }
                    else
                    {
                        return -1;
                    }
                }
            }

            internal readonly Scheduler[] schedulers;

            public Scheduler this[int index] { get { return this.schedulers[index]; } }

            public void Start()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Start();
            }

            public void Activate()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.AllChannelsInitialized();
            }

            public long BlockScheduler(AutoResetEvent selectiveEvent, long val)
            {
                this.wakeUpEvent.Await(selectiveEvent, val);
                return this.wakeUpEvent.Read(); // likely not necessary
            }

            public void WakeUp()
            {
                NaiadTracing.Trace.RegionStart(NaiadTracingRegion.Wakeup);
                if (this.useBroadcastWakeup)
                {
                    this.wakeUpEvent.Advance();
                }
                else
                {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Signal();
                }
                NaiadTracing.Trace.RegionStop(NaiadTracingRegion.Wakeup);
            }

            public void Abort()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Abort();
            }

            public void PauseWithoutRollback()
            {
                using (CountdownEvent pauseCountdown = new CountdownEvent(this.schedulers.Length))
                {
                    lock (this)
                    {
                        foreach (Scheduler scheduler in this.schedulers)
                            scheduler.PauseWithoutRollback(pauseCountdown);
                    }
                    pauseCountdown.Wait();
                }
            }

            public void Pause()
            {
                using (CountdownEvent pauseCountdown = new CountdownEvent(this.schedulers.Length))
                {
                    lock (this)
                    {
                        foreach (Scheduler scheduler in this.schedulers)
                            scheduler.Pause(pauseCountdown);
                    }
                    pauseCountdown.Wait();
                }
            }

            public void SimulateFailure()
            {
                using (CountdownEvent failureCountdown = new CountdownEvent(this.schedulers.Length))
                {
                    lock (this)
                    {
                        foreach (Scheduler scheduler in this.schedulers)
                            scheduler.SimulateFailure(failureCountdown);
                    }
                    failureCountdown.Wait();
                }
            }

            public void Resume()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Resume();
            }

            public void ResumeWithoutRollback()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.ResumeWithoutRollback();
            }

            internal void DrainAllQueuedMessages()
            {
                int dequeued = 0;
                foreach (Scheduler scheduler in this.schedulers)
                    dequeued += scheduler.AcceptWorkItemsFromOthers();
                this.DecrementSharedQueueCount(dequeued);
            }

            #region Scheduler events
            /// <summary>
            /// This event is fired by each worker when it initially starts.
            /// </summary>
            public event EventHandler<WorkerStartArgs> Starting;
            public void NotifyWorkerStarting(Scheduler scheduler)
            {
                if (this.Starting != null)
                    this.Starting(this, new WorkerStartArgs(scheduler.Index));
            }

            /// <summary>
            /// This event is fired by each worker when it wakes from sleeping.
            /// </summary>
            public event EventHandler<WorkerWakeArgs> Waking;
            public void NotifyWorkerWaking(Scheduler scheduler)
            {
                if (this.Waking != null)
                    this.Waking(this, new WorkerWakeArgs(scheduler.Index));
            }

            /// <summary>
            /// This event is fired by a worker immediately before executing a work item.
            /// </summary>
            public event EventHandler<VertexStartArgs> WorkItemStarting;
            public void NotifyVertexStarting(Scheduler scheduler, Scheduler.WorkItem work)
            {
                if (this.WorkItemStarting != null)
                    this.WorkItemStarting(this, new VertexStartArgs(scheduler.Index, work.Vertex.Stage, work.Vertex.VertexId, work.Requirement));
            }

            /// <summary>
            /// This event is fired by a worker immediately after executing a work item.
            /// </summary>
            public event EventHandler<VertexEndArgs> WorkItemEnding;
            public void NotifyVertexEnding(Scheduler scheduler, Scheduler.WorkItem work)
            {
                if (this.WorkItemEnding != null)
                    this.WorkItemEnding(this, new VertexEndArgs(scheduler.Index, work.Vertex.Stage, work.Vertex.VertexId, work.Requirement));
            }

            /// <summary>
            /// This event is fired by a worker immediately after enqueueing a work item.
            /// </summary>
            public event EventHandler<VertexEnqueuedArgs> WorkItemEnqueued;
            public void NotifyVertexEnqueued(Scheduler scheduler, Scheduler.WorkItem work)
            {
                if (this.WorkItemEnqueued != null)
                    this.WorkItemEnqueued(this, new VertexEnqueuedArgs(scheduler.Index, work.Vertex.Stage, work.Vertex.VertexId, work.Requirement));
            }

            /// <summary>
            /// This event is fired by a worker when it becomes idle, because it has no work to execute.
            /// </summary>
            public event EventHandler<WorkerSleepArgs> Sleeping;
            public void NotifySchedulerSleeping(Scheduler scheduler, int queueHighWaterMark)
            {
                if (this.Sleeping != null)
                    this.Sleeping(this, new WorkerSleepArgs(scheduler.Index, queueHighWaterMark));
            }

            /// <summary>
            /// This event is fired by a worker when it has finished all work, and the computation has terminated.
            /// </summary>
            public event EventHandler<WorkerTerminateArgs> Terminating;
            public void NotifySchedulerTerminating(Scheduler scheduler)
            {
                if (this.Terminating != null)
                    this.Terminating(this, new WorkerTerminateArgs(scheduler.Index));
            }

#if false
            /// <summary>
            /// This event is fired by a worker when a batch of records is delivered to an operator.
            /// </summary>
            public event EventHandler<OperatorReceiveArgs> ReceivedRecords;
            public void NotifyOperatorReceivedRecords(Dataflow.Vertex op, int channelId, int recordsReceived)
            {
                if (this.ReceivedRecords != null)
                    this.ReceivedRecords(this, new OperatorReceiveArgs(op.Stage, op.VertexId, channelId, recordsReceived));
            }

            /// <summary>
            /// This event is fired by a worker when a batch of records is sent by an operator.
            /// (N.B. This event is currently not used.)
            /// </summary>
            public event EventHandler<OperatorSendArgs> SentRecords;
            public void NotifyOperatorSentRecords(Dataflow.Vertex op, int channelId, int recordsSent)
            {
                if (this.SentRecords != null)
                    this.SentRecords(this, new OperatorSendArgs(op.Stage, op.VertexId, channelId, recordsSent));
            }
#endif
            #endregion Scheduler events

            internal BaseWorkerGroup(InternalController controller, int numWorkers)
            {
                this.numWorkers = numWorkers;
                this.schedulers = new Scheduler[numWorkers];

                if (controller.Configuration.UseBroadcastWakeup)
                {
                    this.useBroadcastWakeup = true;
                    this.wakeUpEvent = new EventCount();
                }
                else
                    this.useBroadcastWakeup = false;

                for (int i = 0; i < numWorkers; ++i)
                {
                    switch (System.Environment.OSVersion.Platform)
                    {
                        case PlatformID.Win32NT:
                            this.schedulers[i] = new PinnedScheduler(string.Format("Naiad worker {0}", i), i, controller);
                            break;
                        default:
                            this.schedulers[i] = new Scheduler(string.Format("Naiad worker {0}", i), i, controller);
                            break;
                    }
                }
            }
        }

        private readonly BaseWorkerGroup workerGroup;

        /// <summary>
        /// Returns information about the local workers controlled by this controller.
        /// </summary>
        public InternalWorkerGroup Workers { get { return this.workerGroup; } }
        
        public WorkerGroup WorkerGroup { get { return this.workerGroup; } }

        private bool isJoined = false;

        /// <summary>
        /// Blocks until all computation is complete and resources are released.
        /// </summary>
        public void Join()
        {
            List<Exception> graphExceptions = new List<Exception>();

            foreach (var manager in this.baseComputations.Where(x => x.CurrentState == InternalComputationState.Active))
            {
                try
                {
                    manager.Join();
                }
                catch (Exception e)
                {
                    graphExceptions.Add(e);
                }
            }

            this.aborted = true;
            this.restoreEvent.Set();
            this.restoreThread.Join();
            this.pauseEvent.Set();
            this.pauseThread.Join();

            this.workerGroup.Abort();
            
            foreach (Scheduler scheduler in this.workerGroup.schedulers)
                scheduler.Join();

            NotifyOnShutdown();

            this.isJoined = true;

            if (graphExceptions.Count > 0)
                throw new AggregateException(graphExceptions);
        }

        public Task JoinAsync()
        {
            // TODO: Make the use of async more pervasive in the runtime.
            return Task.Factory.StartNew(() => this.Join(), TaskCreationOptions.LongRunning);
        }

        internal long? QueryStatisticAsLong(RuntimeStatistic s)
        {
            long res = 0;
            switch (s)
            {
                // Per scheduler statistics
                case RuntimeStatistic.ProgressLocalRecords:
                case RuntimeStatistic.RxProgressBytes:
                case RuntimeStatistic.RxProgressMessages:
                case RuntimeStatistic.TxProgressBytes:
                case RuntimeStatistic.TxProgressMessages:    
                {
                    for (int i = 0; i < this.workerGroup.schedulers.Length; i++)
                    {
                        res += this.workerGroup.schedulers[i].statistics[(int)s];
                    }
                    break;
                }
                // Network channel receive statistics
                case RuntimeStatistic.RxNetBytes:
                case RuntimeStatistic.RxNetMessages:
                case RuntimeStatistic.TxHighPriorityBytes:
                case RuntimeStatistic.TxHighPriorityMessages:
                case RuntimeStatistic.TxNormalPriorityBytes:
                case RuntimeStatistic.TxNormalPriorityMessages:
                {
                    if (this.NetworkChannel != null)
                    {
                        res = this.NetworkChannel.QueryStatistic(s);
                    }
                    else
                    {
                        return null;
                    }
                    break;
                }                    
                default:
                    return null;
            }
            return res;
        }

        public NetworkChannel NetworkChannel { get { return this.networkChannel; } }

        private readonly NetworkChannel networkChannel;
        private readonly IPEndPoint localEndpoint;

        private Placement defaultPlacement;
        public Placement DefaultPlacement { get { return this.defaultPlacement; } }

        private bool activated;

        /// <summary>
        /// Constructs a controller for a new computation.
        /// </summary>
        /// <param name="config">Controller configuration</param>
        public BaseController(Configuration config)
        {
            this.activated = false;
            this.configuration = config;
            this.HasFailed = false;
            this.SerializationFormat = SerializationFactory.GetCodeGeneratorForVersion(config.SerializerVersion.First, config.SerializerVersion.Second);

            // set up an initial endpoint to try starting the server listening on. If endpoint is null
            // when we call the server constructor, it will choose one by picking an available port to listen on
            IPEndPoint endpoint = null;

            if (this.configuration.Endpoints != null)
            {
                endpoint = this.configuration.Endpoints[this.configuration.ProcessID];
            }

            // if we pass in a null endpoint the server will pick one and return it in the ref arg
            this.server = new NaiadServer(ref endpoint);
            this.localEndpoint = endpoint;

            this.restoreEvent = new AutoResetEvent(false);
            this.restoreThread = new Thread(new ThreadStart(this.RestorationThread));
            this.restoreThread.Start();

            this.pauseEvent = new AutoResetEvent(false);
            this.pauseThread = new Thread(new ThreadStart(this.PauseThread));
            this.pauseThread.Start();

            this.workerGroup = new BaseWorkerGroup(this, config.WorkerCount);

            this.workerGroup.Start();
            this.workerGroup.Activate();

            if (this.configuration.ReadEndpointsFromPPM || this.configuration.Processes > 1)
            {
                this.server.Start();

                if (this.configuration.ReadEndpointsFromPPM)
                {
                    int pId;
                    this.configuration.Endpoints = RegisterAndWaitForPPM(out pId);
                    this.configuration.ProcessID = pId;
                }

                if (this.configuration.Processes > 1)
                {
                    TcpNetworkChannel networkChannel = new TcpNetworkChannel(0, this, config);
                    this.networkChannel = networkChannel;

                    this.server.RegisterNetworkChannel(networkChannel);

                    this.server.AcceptPeerConnections();

                    this.networkChannel.WaitForAllConnections();

                    Logging.Info("Network channel activated");
                }
                else
                {
                    Logging.Info("Configured for single-process operation");
                }
            }

            this.defaultPlacement = new Placement.RoundRobin(this.configuration.Processes, this.workerGroup.Count);

#if DEBUG
            Logging.Progress("Warning: DEBUG build. Not for performance measurements.");
#endif

            if (this.workerGroup.Count < Environment.ProcessorCount)
                Logging.Progress("Warning: Using fewer threads than available processors (use -t to set number of threads).");

            Logging.Progress("Initializing {0} {1}", this.workerGroup.Count, this.workerGroup.Count == 1 ? "thread" : "threads");
            Logging.Progress("Server GC = {0}", System.Runtime.GCSettings.IsServerGC);
            Logging.Progress("GC settings latencymode={0}", System.Runtime.GCSettings.LatencyMode);
            Logging.Progress("Using CLR {0}", System.Environment.Version);

            NaiadTracing.Trace.ProcessInfo(this.configuration.ProcessID, System.Environment.MachineName);
            NaiadTracing.Trace.LockInfo(this.GlobalLock, "Controller lock");
            
            if (this.NetworkChannel != null)
                this.NetworkChannel.StartMessageDelivery();

            this.graphsManaged = 0;
            this.baseComputations = new List<BaseComputation>();
        }

        public Computation NewComputation()
        {
            var result = new BaseComputation(this, this.graphsManaged++);

            this.baseComputations.Add(result);

            for (int i = 0; i < workerGroup.Count; i++)
                workerGroup[i].RegisterGraph(result);

            return result;
        }

        private int graphsManaged;

        /// <summary>
        /// Blocks until all computation associated with the supplied epoch have been retired.
        /// </summary>
        /// <param name="epoch">Epoch to wait for</param>
        public void Sync(int epoch)
        {
            foreach (var manager in this.baseComputations)
                manager.Sync(epoch);
        }

        public void Dispose()
        {
            if (this.activated && !this.isJoined)
            {
                Logging.Error("Attempted to dispose controller before joining.");
                Logging.Error("You must call controller.Join() before disposing/exiting the using block.");
                //System.Environment.Exit(-1);
            }

            foreach (Scheduler scheduler in this.workerGroup.schedulers)
                scheduler.Dispose();

            if (this.networkChannel != null)
                this.networkChannel.Dispose();

            if (this.server != null)
                this.server.Dispose();

            this.restoreEvent.Dispose();
            this.pauseEvent.Dispose();

            Logging.Stop();
        }

        private NaiadServer server;

        private IPEndPoint[] RegisterAndWaitForPPM(out int processID)
            {
            PeloponneseClient client = new PeloponneseClient(this.localEndpoint);
            client.WaitForAllWorkers();
            client.NotifyCleanShutdown();
            processID = client.ThisWorkerIndex;
            return client.WorkerEndpoints;
        }

        private long ticksAtStartup = -1;
        public long TicksSinceStartup { get { return DateTime.Now.Ticks - this.ticksAtStartup; } }

        public void DoStartupBarrier()
        {
            bool oldActived = false;
            this.activated = true;
            if (this.networkChannel != null)
            {
                this.networkChannel.DoStartupBarrier();
                Logging.Progress("Did startup barrier for graph");
            }
            if (oldActived)
            {
                this.ticksAtStartup = DateTime.Now.Ticks;
            }
        }

        public void PauseWithoutRollback()
        {
          SignalPause();
          if (this.networkChannel != null && this.networkChannel is Snapshottable)
          {
            ((Snapshottable)this.networkChannel).AnnounceStopWorld();
            ((Snapshottable)this.networkChannel).WaitForAllWorldStoppedMessages();
          }
          this.workerGroup.DrainAllQueuedMessages();
        }

        public void ResumeWithoutRollback()
        {
          SignalPause();
          if (this.networkChannel != null && this.networkChannel is Snapshottable)
          {
            ((Snapshottable)this.networkChannel).AnnounceResumeWorld();
            ((Snapshottable)this.networkChannel).WaitForAllWorldResumedMessages();
          }
        }

        // public void PauseWithoutRollback()
        // {
        //   if (this.networkChannel != null && this.networkChannel is Snapshottable)
        //   {
        //     ((Snapshottable)this.networkChannel).AnnounceStopWorld();
        //     ((Snapshottable)this.networkChannel).WaitForAllWorldStoppedMessages();
        //   }
        //   this.workerGroup.DrainAllQueuedMessages();
        // }

        // public void ResumeWithoutRollback()
        // {
        //   if (this.networkChannel != null && this.networkChannel is Snapshottable)
        //   {
        //     ((Snapshottable)this.networkChannel).AnnounceResumeWorld();
        //     ((Snapshottable)this.networkChannel).WaitForAllWorldResumedMessages();
        //   }
        // }

        public void Pause(Action<string> logAction)
        {
            Console.WriteLine("Pausing workers");
            logAction("pausing workers");
            this.Workers.Pause();
            logAction("paused workers");
            foreach (BaseComputation computation in this.baseComputations)
            {
                computation.ProgressTracker.ForceFlush();
            }
            logAction("flushed progress");

            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                Console.WriteLine("Announcing pause");
                ((Snapshottable)this.networkChannel).AnnounceWorkersPaused();
                logAction("announced paused");
                ((Snapshottable)this.networkChannel).AnnounceRollbackBarrier();
                logAction("announced barrier");
                ((Snapshottable)this.networkChannel).WaitForAllRollbackBarrierMessages();
                logAction("waited for barrier");
            }
            // XXXXX
            this.workerGroup.DrainAllQueuedMessages();
            logAction("drained messages");
        }

        public bool HasFailed { get; private set; }
        public void TriggerSimulatedFailure(int processId, int restartDelay)
        {
            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                ((Snapshottable)this.networkChannel).AnnounceFailure(processId, restartDelay, true);
            }
        }
        public void ReportSimulatedFailureRestart(int processId)
        {
            foreach (BaseComputation computation in this.baseComputations)
            {
                computation.ReportSimulatedFailureRestart(processId);
            }
        }

        public void SimulateFailure(int delay)
        {
            this.Workers.SimulateFailure();

            this.HasFailed = true;

            Thread.Sleep(delay);

            // on resumption the workers won't actually do anything other than rollbacks and progress traffic
            // since they were set to the restoring state in SimulateFailure above
            this.Workers.Resume();
        }

        public void FlushFinalFaultToleranceTraffic()
        {
            // pick up any fault tolerance messages that were sent after the scheduler paused
            foreach (Scheduler scheduler in this.workerGroup.schedulers)
            {
                scheduler.FlushFaultToleranceTraffic();
            }

            // then flush progress traffic generated by those messages
            foreach (BaseComputation computation in this.baseComputations)
            {
                computation.ProgressTracker.ForceFlush();
            }
        }

        public void ResumeAfterRollback()
        {
            this.HasFailed = false;

            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                ((Snapshottable)this.networkChannel).ResumeAfterRollback();
            }
        }
    }
}
