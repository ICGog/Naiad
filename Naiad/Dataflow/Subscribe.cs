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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;


namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Represents an observable "output" of a Naiad computation, and provides a means
    /// of synchronizing with the computation.
    /// </summary>
    public interface Subscription : IDisposable
    {
        /// <summary>
        /// Blocks the caller until this subscription has processed all inputs up to and
        /// including the given epoch.
        /// </summary>
        /// <param name="time">The epoch.</param>
        /// <remarks>
        /// To synchronize on all subscriptions in a computation at a particular epoch, use the <see cref="Computation.Sync"/> method.
        /// To block until the entire computation has terminated, use the <see cref="Computation.Join"/> method.
        /// </remarks>
        /// <seealso cref="Computation.Sync"/>
        /// <seealso cref="Computation.Join"/>
        void Sync(int time);

        /// <summary>
        /// The stage of the subscription
        /// </summary>
        int StageId { get; }
    }

    /// <summary>
    /// Extension methods
    /// </summary>
    public static class SubscribeExtensionMethods
    {
        /// <summary>
        /// Subscribes to a stream with no callback.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream)
        {
            return stream.Subscribe(x => { });
        }

        /// <summary>
        /// Subscribes to a stream with a per-epoch callback applied by one worker.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream,
                                                Action<IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, new Placement.SingleVertex(0, 0), stream.ForStage.InternalComputation, (j, t, l) => action(l));
        }

        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream,
                                                System.Diagnostics.Stopwatch stopwatch,
                                                Action<IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, new Placement.SingleVertex(0, 0), stream.ForStage.InternalComputation, (j, t, l) => action(l), stopwatch);
        }

        /// <summary>
        /// Subscribes to a stream with a per-epoch callback applied at each worker.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback on worker id and records</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<int, IEnumerable<R>> action)
        {
            return stream.Subscribe((j, t, l) => action(j, l));
        }

        /// <summary>
        /// Subscribes to a stream with a callback parameterized by worker id, epoch, and records.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback on worker id, epoch id, and records</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<int, int, IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, stream.ForStage.Placement, stream.ForStage.InternalComputation, action);
        }

        /// <summary>
        /// Subscribes to a stream with callbacks for record receipt, epoch completion notification, and stream completion notification.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="onRecv">receipt callback</param>
        /// <param name="onNotify">notification callback</param>
        /// <param name="onComplete">completion callback</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Stream<R, Epoch> stream, Action<Message<R, Epoch>, int> onRecv, Action<Epoch, int> onNotify, Action<int> onComplete)
        {
            return new Subscription<R>(stream, stream.ForStage.Placement, onRecv, onNotify, onComplete);
        }
    }
}

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Manages several subscribe vertices, and allows another thread to block until all have completed a specified epoch
    /// </summary>
    internal class Subscription<R> : IDisposable, Subscription
    {
        private readonly Dictionary<int, CountdownEvent> Countdowns;        
        private int LocalVertexCount;

        private int CompleteThrough;

        private bool disposed = false;
        internal bool Disposed { get { return this.disposed; } }

        public void Dispose()
        {
            disposed = true;
        }

        internal readonly InputStage[] SourceInputs;


        /// <summary>
        /// Called by vertices, indicates the receipt of an OnNotify(time)
        /// </summary>
        /// <param name="time">Time that has completed for the vertex</param>
        internal void Signal(Epoch time)
        {
            lock (this.Countdowns)
            {
                // if this is the first mention of time.t, create a new countdown
                if (!this.Countdowns.ContainsKey(time.epoch))
                    this.Countdowns[time.epoch] = new CountdownEvent(this.LocalVertexCount);

                if (this.Countdowns[time.epoch].CurrentCount > 0)
                    this.Countdowns[time.epoch].Signal();
                else
                    Console.Error.WriteLine("Too many Signal({0})", time.epoch);

                // if the last signal, clean up a bit
                if (this.Countdowns[time.epoch].CurrentCount == 0)
                {
                    this.CompleteThrough = time.epoch; // bump completethrough int
                    this.Countdowns.Remove(time.epoch); // remove countdown object
                }
            }
        }

        /// <summary>
        /// Blocks the caller until this subscription has completed the given epoch.
        /// </summary>
        /// <param name="epoch">Time to wait until locally complete</param>
        public void Sync(int epoch)
        {
            CountdownEvent countdown;
            lock (this.Countdowns)
            {
                // if we have already completed it, don't wait
                if (epoch <= this.CompleteThrough)
                    return;

                // if we haven't heard about it, create a new countdown
                if (!this.Countdowns.ContainsKey(epoch))
                    this.Countdowns[epoch] = new CountdownEvent(this.LocalVertexCount);

                countdown = this.Countdowns[epoch];
            }

            // having released the lock, wait.
            countdown.Wait();
        }

        public int StageId { get; private set; }

        internal Subscription(Stream<R, Epoch> input, Placement placement, Action<Message<R, Epoch>, int> onRecv, Action<Epoch, int> onNotify, Action<int> onComplete)
        {
            foreach (var entry in placement)
                if (entry.ProcessId == input.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                    this.LocalVertexCount++;

            var stage = new Stage<SubscribeStreamingVertex<R>, Epoch>(placement, input.ForStage.InternalComputation, Stage.OperatorType.Default, (i, v) => new SubscribeStreamingVertex<R>(i, v, this, onRecv, onNotify, onComplete), "Subscribe");
            stage.SetCheckpointType(CheckpointType.None);

            stage.NewInput(input, (message, vertex) => vertex.OnReceive(message), null);

            this.StageId = stage.StageId;

            this.Countdowns = new Dictionary<int, CountdownEvent>();
            this.CompleteThrough = -1;

            // important for reachability to be defined for the next test
            stage.InternalComputation.Reachability.UpdateReachabilityPartialOrder(stage.InternalComputation);

            // should only schedule next epoch if at least one input who can reach this stage will have data for this.
            this.SourceInputs = stage.InternalComputation.Inputs.Where(i => stage.InternalComputation.Reachability.ComparisonDepth[i.InputId][stage.StageId] != 0).ToArray();

            // add this subscription to the list of outputs.
            stage.InternalComputation.Register(this);
        }

        internal Subscription(Stream<R, Epoch> input, Placement placement, InternalComputation computation, Action<int, int, IEnumerable<R>> action)
        : this(input, placement, computation, action, Stopwatch.StartNew())
        {
        }

        internal Subscription(Stream<R, Epoch> input,
                              Placement placement,
                              InternalComputation computation,
                              Action<int, int, IEnumerable<R>> action,
                              Stopwatch stopwatch)
        {
            foreach (var entry in placement)
                if (entry.ProcessId == computation.Controller.Configuration.ProcessID)
                    this.LocalVertexCount++;

            var stage = new Stage<SubscribeBufferingVertex<R>, Epoch>(placement, computation, Stage.OperatorType.Default, (i, v) => new SubscribeBufferingVertex<R>(i, v, this, action), "Subscribe");
            stage.SetCheckpointType(CheckpointType.Stateless);
            stage.SetCheckpointPolicy(i => new CheckpointWithoutPersistence());

            stage.NewInput(input, (message, vertex) => vertex.OnReceive(message), null);

            this.StageId = stage.StageId;

            this.Countdowns = new Dictionary<int, CountdownEvent>();
            this.CompleteThrough = -1;

            // important for reachability to be defined for the next test
            stage.InternalComputation.Reachability.UpdateReachabilityPartialOrder(stage.InternalComputation);

            // should only schedule next epoch if at least one input who can reach this stage will have data for this.
            this.SourceInputs = stage.InternalComputation.Inputs.Where(i => stage.InternalComputation.Reachability.ComparisonDepth[i.InputId][stage.StageId] != 0).ToArray();

            // add this subscription to the list of outputs.
            stage.InternalComputation.Register(this);
        }
    }

    /// <summary>
    /// Individual subscription vertex, invokes actions and notifies parent stage.
    /// </summary>
    /// <typeparam name="R">Record type</typeparam>
    internal class SubscribeStreamingVertex<R> : SinkVertex<R, Epoch>
    {
        Action<Message<R, Epoch>, int> OnRecv;
        Action<Epoch, int> OnNotifyAction;
        Action<int> OnCompleted;

        Subscription<R> Parent;

        protected override void OnShutdown()
        {
            this.OnCompleted(this.Scheduler.Index);
            base.OnShutdown();
        }

        public override void OnReceive(Message<R, Epoch> record)
        {
            this.OnRecv(record, this.Scheduler.Index);
            this.NotifyAt(record.time);
        }

        /// <summary>
        /// When a time completes, invokes an action on received data, signals parent stage, and schedules OnNotify for next expoch.
        /// </summary>
        /// <param name="time"></param>
        public override void OnNotify(Epoch time)
        {
            // test to see if inputs supplied data for this epoch, or terminated instead
            var validEpoch = false;
            for (int i = 0; i < this.Parent.SourceInputs.Length; i++)
                if (this.Parent.SourceInputs[i].MaximumValidEpoch >= time.epoch)
                    validEpoch = true;
            
            if (validEpoch)
                this.OnNotifyAction(time, this.Scheduler.Index);

            this.Parent.Signal(time);

            if (!this.Parent.Disposed && validEpoch)
                this.NotifyAt(new Epoch(time.epoch + 1));         
        }

        public SubscribeStreamingVertex(int index, Stage<Epoch> stage, Subscription<R> parent, Action<Message<R, Epoch>, int> onrecv, Action<Epoch, int> onnotify, Action<int> oncomplete)
            : base(index, stage)
        {
            this.Parent = parent;

            this.OnRecv = onrecv;
            this.OnNotifyAction = onnotify;
            this.OnCompleted = oncomplete;

            this.PushEventTime(new Epoch(0));
            this.NotifyAt(new Epoch(0));
            this.PopEventTime();
        }
    }

    /// <summary>
    /// Individual subscription vertex, invokes actions and notifies parent stage.
    /// </summary>
    /// <typeparam name="R">Record type</typeparam>
    internal class SubscribeBufferingVertex<R> : SinkBufferingVertex<R, Epoch>
    {
        Action<int, int, IEnumerable<R>> Action;        // (vertexid, epoch, data) => ()
        Subscription<R> Parent;
        Stopwatch stopwatch;
        private int epochsToRelease = -1;
        private readonly Dictionary<Epoch, R[]> cachedOutputs;

        protected override bool MustRollBackPreservingState(Runtime.Progress.Pointstamp[] frontier)
        {
            return true;
        }

        public override void RollBackPreservingState(Runtime.Progress.Pointstamp[] frontier, ICheckpoint<Epoch> lastFullCheckpoint, ICheckpoint<Epoch> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            if (frontier.Length == 0)
            {
                cachedOutputs.Clear();
            }
            else
            {
                if (frontier.Length != 1)
                {
                    throw new ApplicationException("Can only handle epochs");
                }

                int lastEpoch = frontier[0].Timestamp[0];

                var toRemove = this.cachedOutputs.Keys.Where(e => e.epoch > lastEpoch).ToArray();
                foreach (Epoch epoch in toRemove)
                {
                    this.cachedOutputs.Remove(epoch);
                }
            }
        }

        /// <summary>
        /// When a time completes, invokes an action on received data, signals parent stage, and schedules OnNotify for next expoch.
        /// </summary>
        /// <param name="time"></param>
        public override void OnNotify(Epoch time)
        {
            // test to see if inputs supplied data for this epoch, or terminated instead
            var validEpoch = false;
            for (int i = 0; i < this.Parent.SourceInputs.Length; i++)
                if (this.Parent.SourceInputs[i].MaximumValidEpoch >= time.epoch)
                    validEpoch = true;

            if (this.LoggingEnabled)
            {
                if (validEpoch)
                {
                    if (time.epoch <= this.epochsToRelease)
                    {
                        this.Action(this.VertexId, time.epoch, Input.GetRecordsAt(time));
                    }
                    else
                    {
                        Console.WriteLine("{0} Caching output {1}",
                                          stopwatch.ElapsedMilliseconds, time);
                        this.cachedOutputs.Add(time, Input.GetRecordsAt(time).ToArray());
                    }
                }
            }
            else
            {
                if (validEpoch)
                {
                    this.Action(this.VertexId, time.epoch, Input.GetRecordsAt(time));
                }

                this.Parent.Signal(time);
            }

            if (!this.Parent.Disposed && validEpoch)
                this.NotifyAt(new Epoch(time.epoch + 1));
        }

        public override void NotifyGarbageCollectionFrontier(Runtime.Progress.Pointstamp[] frontier)
        {
            if (frontier.Length != 1 || frontier[0].Timestamp.Length != 1)
            {
                throw new ApplicationException("Must use epochs");
            }

            this.epochsToRelease = frontier[0].Timestamp[0];
            var timesToRelease = this.cachedOutputs.Where(time => time.Key.epoch <= this.epochsToRelease).OrderBy(time => time.Key.epoch).ToArray();

            foreach (var time in timesToRelease)
            {
                Console.WriteLine("{0} Caching output remove {1}",
                                  stopwatch.ElapsedMilliseconds, time.Key);
                this.cachedOutputs.Remove(time.Key);
                this.Action(this.VertexId, time.Key.epoch, time.Value);
                this.Parent.Signal(time.Key);
            }
        }

        protected override void InitializeRestoration(Runtime.Progress.Pointstamp[] frontier)
        {
            int epochsToKeep = -1;
            if (frontier.Length != 0)
            {
                if (frontier.Length != 1 || frontier[0].Timestamp.Length != 1)
                {
                    throw new ApplicationException("Must use epochs");
                }
                epochsToKeep = frontier[0].Timestamp[0];
            }

            var timesToDiscard = this.cachedOutputs.Where(time => time.Key.epoch > epochsToKeep).ToArray();

            foreach (var time in timesToDiscard)
            {
                this.cachedOutputs.Remove(time.Key);
            }
        }

        public SubscribeBufferingVertex(int index, Stage<Epoch> stage, Subscription<R> parent, Action<int, int, IEnumerable<R>> action)
            : base(index, stage, null)
        {
            this.Parent = parent;
            this.Action = action;
            this.Input = new VertexInputBuffer<R, Epoch>(this);
            Epoch eventTime = new Epoch(0);
            this.PushEventTime(eventTime);
            this.NotifyAt(eventTime);
            this.PopEventTime();
            this.stopwatch = Stopwatch.StartNew();
            this.cachedOutputs = new Dictionary<Epoch, R[]>();
        }

        public SubscribeBufferingVertex(Stopwatch stopwatch, int index, Stage<Epoch> stage, Subscription<R> parent, Action<int, int, IEnumerable<R>> action)
            : base(index, stage, null)
        {
            this.Parent = parent;
            this.Action = action;
            this.Input = new VertexInputBuffer<R, Epoch>(this);
            Epoch eventTime = new Epoch(0);
            this.PushEventTime(eventTime);
            this.NotifyAt(eventTime);
            this.PopEventTime();
            this.stopwatch = stopwatch;
            this.cachedOutputs = new Dictionary<Epoch, R[]>();
        }

    }
}
