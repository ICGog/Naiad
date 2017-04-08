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
using System.Text;


using System.Collections.Concurrent;
using System.IO;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace;

using System.Linq.Expressions;
using System.Diagnostics;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.OperatorImplementations
{
    internal class BinaryStatefulOperator<K, V1, V2, S1, S2, T, R> : BinaryBufferingVertex<Weighted<S1>, Weighted<S2>, Weighted<R>, T>
        where K : IEquatable<K>
        where V1 : IEquatable<V1>
        where V2 : IEquatable<V2>
        where S1 : IEquatable<S1>
        where S2 : IEquatable<S2>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public override void OnReceive1(Message<Weighted<S1>, T> message)
        {
            if (this.inputImmutable1)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                    this.OnInput1(message.payload[i], message.time);
            }
            else
                base.OnReceive1(message);
        }

        public override void OnReceive2(Message<Weighted<S2>, T> message)
        {
            if (this.inputImmutable2)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                    this.OnInput2(message.payload[i], message.time);
            }
            else
                base.OnReceive2(message);
        }

        protected override void OnShutdown()
        {
            base.OnShutdown();

            if (inputTrace1 != null)
                inputTrace1.Release();
            inputTrace1 = null;

            if (inputTrace2 != null)
                inputTrace2.Release();
            inputTrace2 = null;

            if (outputTrace != null)
                outputTrace.Release();
            outputTrace = null;

            keyIndices = null;
            curKeyIndices = null;
            keysToProcess = null;
            internTable = null;

        }

        protected override void SetPotentialRollbackRange(ICheckpoint<T> stableRange, ICheckpoint<T> rollbackRange)
        {
            this.checkpointManager.StableTimeRange = stableRange;
            this.checkpointManager.RollbackTimeRange = rollbackRange;
        }

        protected override void UpdateReachability(List<Pointstamp> versions)
        {
            base.UpdateReachability(versions);

            if (versions != null && internTable != null)
            {
                int staleTimesCount = internTable.UpdateReachability(versions,
                    t => this.checkpointManager.StableTimeRange.ContainsTime(t),
                    t => this.checkpointManager.RollbackTimeRange.ContainsTime(t));

                if (staleTimesCount > this.Stage.Computation.Controller.Configuration.MaxLatticeInternStaleTimes)
                {
                    this.CompactInternTable();
                }
            }
        }

        //public readonly Naiad.Frameworks.RecvFiberBank<Weighted<S1>, T> LeftInput;
        //public readonly Naiad.Frameworks.RecvFiberBank<Weighted<S2>, T> RightInput;

        //public override FiberRecvEndpoint<Weighted<S1>, T> LeftInput { get { return new LeftRecvEndpoint(this); } }
        //public override FiberRecvEndpoint<Weighted<S2>, T> RightInput { get { return new RightRecvEndpoint(this); } }

        public Func<S1, K> key1;      // extracts the key from the input record
        public Func<S1, V1> value1;    // reduces input record to relevant value

        public Func<S2, K> key2;      // extracts the key from the input record
        public Func<S2, V2> value2;    // reduces input record to relevant value

        public Expression<Func<S1, K>> keyExpression1;      // extracts the key from the input record
        public Expression<Func<S1, V1>> valueExpression1;    // reduces input record to relevant value

        public Expression<Func<S2, K>> keyExpression2;      // extracts the key from the input record
        public Expression<Func<S2, V2>> valueExpression2;    // reduces input record to relevant value

        readonly bool MaintainOutputTrace;

        protected CollectionTraceWithHeap<R> createOutputTrace()
        {
            return new CollectionTraceWithHeap<R>((x, y) => internTable.LessThan(x, y), x => internTable.UpdateTime(x), this.Stage.Placement.Count);
        }

        protected CollectionTraceCheckpointable<V2> createInputTrace2()
        {
            if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(keyExpression2, valueExpression2))
            {
                if (this.inputImmutable2)
                {
                    Logging.Progress("Allocating immutable no heap in {0}", this);
                    return new CollectionTraceImmutableNoHeap<V2>();
                }
                else
                    return new CollectionTraceWithoutHeap<V2>((x, y) => internTable.LessThan(x, y),
                                                                   x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable2)
                {
                    Logging.Progress("Allocating immutable heap in {0}", this);
                    return new CollectionTraceImmutable<V2>();
                }
                else
                    return new CollectionTraceWithHeap<V2>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        protected CollectionTraceCheckpointable<V1> createInputTrace1()
        {
            if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(keyExpression1, valueExpression1))
            {
                if (this.inputImmutable1)
                    return new CollectionTraceImmutableNoHeap<V1>();
                else
                    return new CollectionTraceWithoutHeap<V1>((x, y) => internTable.LessThan(x, y),
                                                                    x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable1)
                {
                    Logging.Progress("Allocating immutable heap in {0}", this);
                    return new CollectionTraceImmutable<V1>();
                }
                else
                    return new CollectionTraceWithHeap<V1>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        public override void OnNotify(T workTime)
        {
            if (!this.inputImmutable1)
            {
                foreach (var record in this.Input1.GetRecordsAt(workTime))
                    OnInput1(record, workTime);
            }

            if (!this.inputImmutable2)
            {
                foreach (var record in this.Input2.GetRecordsAt(workTime))
                    OnInput2(record, workTime);
            }

            Compute();
            Flush();

            //if (this.inputImmutable1 && this.inputImmutable2)
            //    this.ShutDown();
            //else
            {
                if (inputTrace1 != null) inputTrace1.Compact();
                if (inputTrace2 != null) inputTrace2.Compact();
            }
        }

        protected Dictionary<K, BinaryKeyIndices> keyIndices;
        protected Dictionary<K, BinaryKeyIndices> curKeyIndices;

        protected CollectionTraceCheckpointable<V1> inputTrace1;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<V2> inputTrace2;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<R> outputTrace;             // collects outputs

        protected int outputWorkspace = 0;

        protected LatticeInternTable<T> internTable = new LatticeInternTable<T>();

        protected NaiadList<K> keysToProcess = new NaiadList<K>(1);

        public virtual void OnInput1(Weighted<S1> entry, T time)
        {
            var k = key1(entry.record);

            BinaryKeyIndices state;
            if (!keyIndices.TryGetValue(k, out state))
                state = new BinaryKeyIndices();

            if (state.unprocessed1 == 0 && state.unprocessed2 == 0)
                keysToProcess.Add(k);

            inputTrace1.Introduce(ref state.unprocessed1, value1(entry.record), entry.weight, internTable.Intern(time));

            keyIndices[k] = state;
            curKeyIndices[k] = state;
        }

        public virtual void OnInput2(Weighted<S2> entry, T time)
        {
            var k = key2(entry.record);

            BinaryKeyIndices state;
            if (!keyIndices.TryGetValue(k, out state))
                state = new BinaryKeyIndices();

            if (state.unprocessed1 == 0 && state.unprocessed2 == 0)
                keysToProcess.Add(k);

            inputTrace2.Introduce(ref state.unprocessed2, value2(entry.record), entry.weight, internTable.Intern(time));

            keyIndices[k] = state;
            curKeyIndices[k] = state;
        }

        public virtual void Compute()
        {
            for (int i = 0; i < keysToProcess.Count; i++)
                Update(keysToProcess.Array[i]);

            keysToProcess.Clear();
        }

        protected NaiadList<Weighted<V1>> collection1 = new NaiadList<Weighted<V1>>(1);
        protected NaiadList<Weighted<V1>> difference1 = new NaiadList<Weighted<V1>>(1);

        protected NaiadList<Weighted<V2>> collection2 = new NaiadList<Weighted<V2>>(1);
        protected NaiadList<Weighted<V2>> difference2 = new NaiadList<Weighted<V2>>(1);


        // Moves from unprocessed[key] to processed[key], updating output[key] and Send()ing.
        protected virtual void Update(K key)
        {
            BinaryKeyIndices traceIndices;
            bool present = this.keyIndices.TryGetValue(key, out traceIndices);

            if (present && (traceIndices.unprocessed1 != 0 || traceIndices.unprocessed2 != 0))
            {
                inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.processed1);
                inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.processed2);

                // iterate through the times that may require updates.
                var interestingTimes = InterestingTimes(traceIndices);

                // incorporate the updates, so we can compare old and new outputs.
                inputTrace1.IntroduceFrom(ref traceIndices.processed1, ref traceIndices.unprocessed1, false);
                inputTrace2.IntroduceFrom(ref traceIndices.processed2, ref traceIndices.unprocessed2, false);

                for (int i = 0; i < interestingTimes.Count; i++)
                    UpdateTime(key, traceIndices, interestingTimes.Array[i]);

                // clean out the state we just processed
                inputTrace1.ZeroState(ref traceIndices.unprocessed1);
                inputTrace2.ZeroState(ref traceIndices.unprocessed2);

                // move the differences we produced from local to persistent storage.
                if (MaintainOutputTrace)
                {
                    outputTrace.IntroduceFrom(ref traceIndices.output, ref outputWorkspace);
                    outputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.output);
                }
                else
                    outputTrace.ZeroState(ref outputWorkspace);


                keyIndices[key] = traceIndices;
                curKeyIndices[key] = traceIndices;
            }
        }

        protected NaiadList<int> timeList = new NaiadList<int>(1);
        protected NaiadList<int> truthList = new NaiadList<int>(1);
        protected NaiadList<int> deltaList = new NaiadList<int>(1);

        protected virtual NaiadList<int> InterestingTimes(BinaryKeyIndices keyIndex)
        {
            deltaList.Clear();
            inputTrace1.EnumerateTimes(keyIndex.unprocessed1, deltaList);
            inputTrace2.EnumerateTimes(keyIndex.unprocessed2, deltaList);

            truthList.Clear();
            inputTrace1.EnumerateTimes(keyIndex.processed1, truthList);
            inputTrace2.EnumerateTimes(keyIndex.processed2, truthList);

            timeList.Clear();
            this.internTable.InterestingTimes(timeList, truthList, deltaList);

            return timeList;
        }

        protected NaiadList<Weighted<R>> outputCollection = new NaiadList<Weighted<R>>(1);

        protected virtual void UpdateTime(K key, BinaryKeyIndices keyIndex, int timeIndex)
        {
            // subtract out prior records before adding new ones
            outputTrace.SubtractStrictlyPriorDifferences(ref outputWorkspace, timeIndex);

            NewOutputMinusOldOutput(key, keyIndex, timeIndex);

            var outputTime = this.internTable.times[timeIndex];

            outputCollection.Clear();
            outputTrace.EnumerateDifferenceAt(outputWorkspace, timeIndex, outputCollection);

            var output = this.Output.GetBufferForTime(outputTime);

            for (int i = 0; i < outputCollection.Count; i++)
                output.Send(outputCollection.Array[i]);
        }

        protected virtual void NewOutputMinusOldOutput(K key, BinaryKeyIndices keyIndex, int timeIndex)
        {
            Reduce(key, keyIndex, timeIndex);

            // this suggests we want to init updateToOutput with -output
            outputCollection.Clear();
            outputTrace.EnumerateCollectionAt(keyIndex.output, timeIndex, outputCollection);
            for (int i = 0; i < outputCollection.Count; i++)
                outputTrace.Introduce(ref outputWorkspace, outputCollection.Array[i].record, -outputCollection.Array[i].weight, timeIndex);
        }

        // expected to populate resultList to match reduction(collection.source)
        protected virtual void Reduce(K key, BinaryKeyIndices keyIndex, int time) { }

        /* Checkpoint format:
         * bool terminated
         * if !terminated:
         *     LatticeInternTable<T>                            internTable
         *     CollectionTrace<>                                inputTrace1
         *     CollectionTrace<>                                inputTrace2
         *     CollectionTrace<>                                outputTrace
         *     Dictionary<K,KeyIndices>                         keysToProcess
         *     int                                              recordsToProcessCount1
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess1
         *     int                                              recordsToProcessCount2
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess2
         */

        protected override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            writer.Write(this.IsShutDown);
            if (!this.IsShutDown)
            {
                this.internTable.Checkpoint(writer);
                this.inputTrace1.Checkpoint(writer);
                this.inputTrace2.Checkpoint(writer);
                this.outputTrace.Checkpoint(writer);

                this.keyIndices.Checkpoint(writer);

                this.Input1.Checkpoint(writer);
                this.Input2.Checkpoint(writer);

                /*
                writer.Write(this.recordsToProcess1.Count, PrimitiveSerializers.Int32);
                foreach (KeyValuePair<T, NaiadList<Weighted<S1>>> kvp in this.recordsToProcess1)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, weightedS1Serializer);
                }

                writer.Write(this.recordsToProcess2.Count, PrimitiveSerializers.Int32);
                foreach (KeyValuePair<T, NaiadList<Weighted<S2>>> kvp in this.recordsToProcess2)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, weightedS2Serializer);
                }
                 */
            }
        }

        protected override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                this.internTable.Restore(reader);
                this.inputTrace1.Restore(reader);
                this.inputTrace2.Restore(reader);
                this.outputTrace.Restore(reader);

                this.keyIndices.Restore(reader);

                this.Input1.Restore(reader);
                this.Input2.Restore(reader);

                /*
                int recordsToProcessCount1 = reader.Read<int>(PrimitiveSerializers.Int32);

                foreach (NaiadList<Weighted<S1>> recordList in this.recordsToProcess1.Values)
                    recordList.Free();
                this.recordsToProcess1.Clear();

                for (int i = 0; i < recordsToProcessCount1; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    NaiadList<Weighted<S1>> value = new NaiadList<Weighted<S1>>();
                    value.Restore(reader, weightedS1Serializer);
                    this.recordsToProcess1[key] = value;
                }

                int recordsToProcessCount2 = reader.Read<int>(PrimitiveSerializers.Int32);

                foreach (NaiadList<Weighted<S2>> recordList in this.recordsToProcess2.Values)
                    recordList.Free();
                this.recordsToProcess2.Clear();

                for (int i = 0; i < recordsToProcessCount2; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    NaiadList<Weighted<S2>> value = new NaiadList<Weighted<S2>>();
                    value.Restore(reader, weightedS2Serializer);
                    this.recordsToProcess2[key] = value;
                }
                 */
            }
        }

        // private bool HasStateInCheckpoint(BinaryKeyIndices indices, ICheckpoint<T> checkpoint)
        // {
        //     // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

        //     NaiadList<int> timeList = new NaiadList<int>(16);
        //     this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
        //     for (int i = 0; i < timeList.Count; ++i)
        //     {
        //         if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
        //         {
        //             return true;
        //         }
        //     }

        //     timeList.Clear();
        //     this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
        //     for (int i = 0; i < timeList.Count; ++i)
        //     {
        //         if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
        //         {
        //             return true;
        //         }
        //     }

        //     timeList.Clear();
        //     this.outputTrace.EnumerateTimes(indices.output, timeList);
        //     for (int i = 0; i < timeList.Count; ++i)
        //     {
        //         if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
        //         {
        //             return true;
        //         }
        //     }

        //     return false;
        // }

        private Pair<Pair<int, int>, int> CountTimesInCheckpoint(BinaryKeyIndices indices, ICheckpoint<T> checkpoint)
        {
            // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

            NaiadList<int> timeList = new NaiadList<int>(16);
            this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
            int input1Count = 0;
            for (int i = 0; i < timeList.Count; ++i)
            {
                if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                {
                    ++input1Count;
                }
            }

            timeList.Clear();
            this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
            int input2Count = 0;
            for (int i = 0; i < timeList.Count; ++i)
            {
                if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                {
                    ++input2Count;
                }
            }

            timeList.Clear();
            this.outputTrace.EnumerateTimes(indices.output, timeList);
            int outputCount = 0;
            for (int i = 0; i < timeList.Count; ++i)
            {
                if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                {
                    ++outputCount;
                }
            }

            return input1Count.PairWith(input2Count).PairWith(outputCount);
        }

        protected readonly CheckpointHelpers.IncrementalCheckpointManager<T> checkpointManager;

        protected override bool NextCheckpointIsIncremental(ICheckpoint<T> candidateIncrementalCheckpoint)
        {
            if (this.isShutdown || this.checkpointManager.ForceFullCheckpoint)
            {
                return false;
            }

            long entries = this.CountEntries(candidateIncrementalCheckpoint);

            return this.checkpointManager.NextCheckpointIsIncremental(entries);
        }

        protected virtual long CountEntries(ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            if (!this.isShutdown)
            {
                foreach (var indices in this.curKeyIndices.Values)
                {
                    checkpointEntries +=
                        this.inputTrace1.CountEntries(indices.processed1, checkpoint, this.internTable.times, true, false).First;
                    checkpointEntries +=
                        this.inputTrace2.CountEntries(indices.processed2, checkpoint, this.internTable.times, true, false).First;
                    checkpointEntries +=
                        this.outputTrace.CountEntries(indices.output, checkpoint, this.internTable.times, true, false).First;
                }
            }

            return checkpointEntries;
        }

        protected override void Checkpoint(NaiadWriter writer, ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                if (checkpoint.IsFullCheckpoint)
                {
                    this.CompactInternTable();
                    this.curKeyIndices = keyIndices;
                }

                List<Pair<Pair<K, BinaryKeyIndices>, Pair<Pair<int, int>, int>>> indicesTimeCounts =
                  new List<Pair<Pair<K, BinaryKeyIndices>, Pair<Pair<int, int>, int>>>();

                foreach (var key in this.curKeyIndices)
                {
                  BinaryKeyIndices indices = key.Value;
                  Pair<Pair<int, int>, int> timeCount =
                    this.CountTimesInCheckpoint(indices, checkpoint);
                  if (timeCount.First.First > 0 ||
                      timeCount.First.Second > 0 ||
                      timeCount.Second > 0)
                  {
                    indicesTimeCounts.Add((key.Key.PairWith(indices)).PairWith(timeCount));
                  }
                }
                writer.Write(indicesTimeCounts.Count);
                foreach (var indicesTimeCount in indicesTimeCounts)
                {
                  writer.Write(indicesTimeCount.First.First);
                  writer.Write(indicesTimeCount.Second.First.First);
                  checkpointEntries +=
                    this.inputTrace1.CheckpointKey(indicesTimeCount.First.Second.processed1,
                                                   checkpoint,
                                                   this.internTable.times,
                                                   writer);
                  writer.Write(indicesTimeCount.Second.First.Second);
                  checkpointEntries +=
                    this.inputTrace2.CheckpointKey(indicesTimeCount.First.Second.processed2,
                                                   checkpoint,
                                                   this.internTable.times,
                                                   writer);
                  writer.Write(indicesTimeCount.Second.Second);
                  checkpointEntries +=
                    this.outputTrace.CheckpointKey(indicesTimeCount.First.Second.output,
                                                   checkpoint,
                                                   this.internTable.times,
                                                   writer);
                }

                // int compactedCount = 0;
                // foreach (var indices in this.keyIndices.Values)
                // {
                //     if (this.HasStateInCheckpoint(indices, checkpoint))
                //     {
                //         ++compactedCount;
                //     }
                // }

                // writer.Write(compactedCount);
                // foreach (var key in this.keyIndices)
                // {
                //     BinaryKeyIndices indices = key.Value;

                //     Pair<Pair<int, int>, int> timeCounts = this.CountTimesInCheckpoint(indices, checkpoint);

                //     if (timeCounts.First.First > 0 || timeCounts.First.Second > 0 || timeCounts.Second > 0)
                //     {
                //         writer.Write(key.Key);

                //         writer.Write(timeCounts.First.First);
                //         checkpointEntries +=
                //             this.inputTrace1.CheckpointKey(indices.processed1, checkpoint, this.internTable.times, writer);

                //         writer.Write(timeCounts.First.Second);
                //         checkpointEntries +=
                //             this.inputTrace2.CheckpointKey(indices.processed2, checkpoint, this.internTable.times, writer);

                //         writer.Write(timeCounts.Second);
                //         checkpointEntries +=
                //             this.outputTrace.CheckpointKey(indices.output, checkpoint, this.internTable.times, writer);
                //     }
                // }

                this.keysToProcess.Checkpoint(writer);
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        protected override bool CanRollBackPreservingState(Pointstamp[] frontier)
        {
            // we are careful not to coalesce times in the potential rollback range, so we are able to roll back without
            // restoring from a checkpoint.
            return true;
        }

        public void RollBackBasePreservingState(Pointstamp[] frontier, ICheckpoint<T> lastFullCheckpoint, ICheckpoint<T> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);
        }

        public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<T> lastFullCheckpoint, ICheckpoint<T> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            if (!this.isShutdown)
            {
                bool countFull = !this.checkpointManager.CachedFullCheckpoint.Equals(lastFullCheckpoint);
                bool countIncremental = countFull || !this.checkpointManager.CachedIncrementalCheckpoint.Equals(lastIncrementalCheckpoint);

                Pair<long, long> counts = 0L.PairWith(0L);

                if (frontier.Length == 0)
                {
                    this.InitializeRestoration(frontier);
                }
                else
                {
                    ICheckpoint<T> timeRange = FrontierCheckpointTester<T>.CreateDownwardClosed(frontier);

                    foreach (var key in this.keyIndices.Keys.ToArray())
                    {
                        BinaryKeyIndices indices = this.keyIndices[key];

                        this.inputTrace1.RemoveStateInTimes(ref indices.processed1, t => timeRange.ContainsTime(this.internTable.times[t]));
                        this.inputTrace1.ZeroState(ref indices.unprocessed1);
                        this.inputTrace2.RemoveStateInTimes(ref indices.processed2, t => timeRange.ContainsTime(this.internTable.times[t]));
                        this.inputTrace2.ZeroState(ref indices.unprocessed2);
                        this.outputTrace.RemoveStateInTimes(ref indices.output, t => timeRange.ContainsTime(this.internTable.times[t]));

                        if (countFull || countIncremental)
                        {
                            Pair<long, long> thisCounts;
                            thisCounts = this.inputTrace1.CountEntries(indices.processed1, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                            thisCounts = this.inputTrace2.CountEntries(indices.processed2, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                            thisCounts = this.outputTrace.CountEntries(indices.output, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                        }

                        this.keyIndices[key] = indices;
                    }

                    if (!countFull)
                    {
                        counts.First = this.checkpointManager.CachedFullCheckpointEntries;
                    }

                    if (!countIncremental)
                    {
                        counts.Second = this.checkpointManager.CachedIncrementalCheckpointEntries;
                    }

                    this.checkpointManager.RegisterCheckpoint(counts.First, lastFullCheckpoint, counts.Second, lastIncrementalCheckpoint);
                }
                this.curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            }
        }

        internal virtual void CompactInternTable()
        {
            if (!this.isShutdown)
            {
                LatticeInternTable<T> newInternTable = new LatticeInternTable<T>();
                bool[] usedTimes = new bool[this.internTable.count];

                foreach (var key in this.keyIndices.Keys.ToArray())
                {
                    BinaryKeyIndices indices = this.keyIndices[key];
                    this.inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed1);
                    this.inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.unprocessed1);
                    this.inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed2);
                    this.inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.unprocessed2);
                    this.outputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.output);
                    if (indices.IsEmpty)
                    {
                        this.keyIndices.Remove(key);
                        this.curKeyIndices.Remove(key);
                    }
                    else
                    {
                        this.keyIndices[key] = indices;
                        if (this.curKeyIndices.ContainsKey(key))
                          this.curKeyIndices[key] = indices;

                        this.inputTrace1.MarkUsedTimes(indices.processed1, usedTimes);
                        this.inputTrace1.MarkUsedTimes(indices.unprocessed1, usedTimes);
                        this.inputTrace2.MarkUsedTimes(indices.processed2, usedTimes);
                        this.inputTrace2.MarkUsedTimes(indices.unprocessed2, usedTimes);
                        this.outputTrace.MarkUsedTimes(indices.output, usedTimes);
                    }
                }

                int[] transferTime = new int[this.internTable.count];
                for (int i=0; i<this.internTable.count; ++i)
                {
                    if (usedTimes[i])
                    {
                        transferTime[i] = newInternTable.Intern(this.internTable.times[i]);
                    }
                }

                foreach (var indices in this.keyIndices.Values)
                {
                    this.inputTrace1.TransferTimesToNewInternTable(indices.processed1, transferTime);
                    this.inputTrace1.TransferTimesToNewInternTable(indices.unprocessed1, transferTime);
                    this.inputTrace2.TransferTimesToNewInternTable(indices.processed2, transferTime);
                    this.inputTrace2.TransferTimesToNewInternTable(indices.unprocessed2, transferTime);
                    this.outputTrace.TransferTimesToNewInternTable(indices.output, transferTime);
                }

                this.inputTrace1.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.inputTrace2.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.outputTrace.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.internTable = newInternTable;
            }
        }

        protected override void InitializeRestoration(Pointstamp[] frontier)
        {
            // empty all the state
            this.keyIndices = new Dictionary<K, BinaryKeyIndices>();
            this.curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            this.keysToProcess.Clear();
            this.internTable = new LatticeInternTable<T>();
            this.inputTrace1 = createInputTrace1();
            this.inputTrace2 = createInputTrace2();
            this.outputTrace = createOutputTrace();
            this.checkpointManager.ForceFullCheckpoint = true;
        }

        protected override void RestorePartialCheckpoint(NaiadReader reader, ICheckpoint<T> checkpoint)
        {
            this.curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            long checkpointEntries = 0;

            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                int numberOfKeys = reader.Read<int>();
                for (int i = 0; i < numberOfKeys; ++i)
                {
                    K key = reader.Read<K>();

                    BinaryKeyIndices indices;
                    if (!this.keyIndices.TryGetValue(key, out indices))
                    {
                        indices = new BinaryKeyIndices();
                    }

                    checkpointEntries += this.inputTrace1.RestoreKey(ref indices.processed1, this.internTable, reader);
                    checkpointEntries += this.inputTrace2.RestoreKey(ref indices.processed2, this.internTable, reader);
                    checkpointEntries += this.outputTrace.RestoreKey(ref indices.output, this.internTable, reader);

                    this.keyIndices[key] = indices;
                    this.curKeyIndices[key] = indices;
                }

                this.keysToProcess.Restore(reader, false);
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        protected readonly bool inputImmutable1 = false;
        protected readonly bool inputImmutable2 = false;

        public BinaryStatefulOperator(int index, Stage<T> stage, bool input1Immutable, bool input2Immutable, Expression<Func<S1, K>> k1, Expression<Func<S2, K>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, bool maintainOC = true)
            : base(index, stage, null)
        {
            key1 = k1.Compile();
            value1 = v1.Compile();

            key2 = k2.Compile();
            value2 = v2.Compile();

            keyExpression1 = k1;
            keyExpression2 = k2;
            valueExpression1 = v1;
            valueExpression2 = v2;

            MaintainOutputTrace = maintainOC;

            inputImmutable1 = input1Immutable;
            inputImmutable2 = input2Immutable;

            keyIndices = new Dictionary<K, BinaryKeyIndices>();
            curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            inputTrace1 = createInputTrace1();
            inputTrace2 = createInputTrace2();
            outputTrace = createOutputTrace();

            outputWorkspace = 0;

            this.checkpointManager = new CheckpointHelpers.IncrementalCheckpointManager<T>(this.Stage);
        }
    }

    internal class ConservativeBinaryStatefulOperator<K, V1, V2, S1, S2, T, R> : BinaryBufferingVertex<Weighted<S1>, Weighted<S2>, Weighted<R>, T>
        where K : IEquatable<K>
        where V1 : IEquatable<V1>
        where V2 : IEquatable<V2>
        where S1 : IEquatable<S1>
        where S2 : IEquatable<S2>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public override void OnReceive1(Message<Weighted<S1>, T> message)
        {
            if (this.inputImmutable1)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                    this.OnInput1(message.payload[i], message.time);
            }
            else
                base.OnReceive1(message);
        }

        public override void OnReceive2(Message<Weighted<S2>, T> message)
        {
            if (this.inputImmutable2)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    this.OnInput2(message.payload[i], message.time);
                }
            }
            else
                base.OnReceive2(message);
        }

        protected override void OnShutdown()
        {
            base.OnShutdown();

            if (inputTrace1 != null)
                inputTrace1.Release();
            inputTrace1 = null;

            if (inputTrace2 != null)
                inputTrace2.Release();
            inputTrace2 = null;

            if (outputTrace != null)
                outputTrace.Release();
            outputTrace = null;

            keyIndices = null;
            curKeyIndices = null;
            KeysToProcessAtTimes = null;
            internTable = null;

        }

        protected override void SetPotentialRollbackRange(ICheckpoint<T> stableRange, ICheckpoint<T> rollbackRange)
        {
            this.checkpointManager.StableTimeRange = stableRange;
            this.checkpointManager.RollbackTimeRange = rollbackRange;
        }

        protected override void UpdateReachability(List<Pointstamp> versions)
        {
            base.UpdateReachability(versions);

            if (versions != null && internTable != null)
            {
                int staleTimesCount = internTable.UpdateReachability(versions,
                    t => this.checkpointManager.StableTimeRange.ContainsTime(t),
                    t => this.checkpointManager.RollbackTimeRange.ContainsTime(t));

                if (staleTimesCount > this.Stage.Computation.Controller.Configuration.MaxLatticeInternStaleTimes)
                {
                    this.CompactInternTable();
                }
            }
        }

        public Func<S1, K> key1;      // extracts the key from the input record
        public Func<S1, V1> value1;    // reduces input record to relevant value

        public Func<S2, K> key2;      // extracts the key from the input record
        public Func<S2, V2> value2;    // reduces input record to relevant value

        public Expression<Func<S1, K>> keyExpression1;      // extracts the key from the input record
        public Expression<Func<S1, V1>> valueExpression1;    // reduces input record to relevant value

        public Expression<Func<S2, K>> keyExpression2;      // extracts the key from the input record
        public Expression<Func<S2, V2>> valueExpression2;    // reduces input record to relevant value

        protected CollectionTraceWithHeap<R> createOutputTrace()
        {
            return new CollectionTraceWithHeap<R>((x, y) => internTable.LessThan(x, y), x => internTable.UpdateTime(x), this.Stage.Placement.Count);
        }

        protected CollectionTraceCheckpointable<V2> createInputTrace2()
        {
            if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(keyExpression2, valueExpression2))
            {
                if (this.inputImmutable2)
                {
                    Logging.Progress("Allocating immutable no heap in {0}", this);
                    return new CollectionTraceImmutableNoHeap<V2>();
                }
                else
                    return new CollectionTraceWithoutHeap<V2>((x, y) => internTable.LessThan(x, y),
                                                                   x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable2)
                {
                    Logging.Progress("Allocating immutable heap in {0}", this);
                    return new CollectionTraceImmutable<V2>();
                }
                else
                    return new CollectionTraceWithHeap<V2>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        protected CollectionTraceCheckpointable<V1> createInputTrace1()
        {
            if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(keyExpression1, valueExpression1))
            {
                if (this.inputImmutable1)
                    return new CollectionTraceImmutableNoHeap<V1>();
                else
                    return new CollectionTraceWithoutHeap<V1>((x, y) => internTable.LessThan(x, y),
                                                                    x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable1)
                {
                    Logging.Progress("Allocating immutable heap in {0}", this);
                    return new CollectionTraceImmutable<V1>();
                }
                else
                    return new CollectionTraceWithHeap<V1>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        public override void OnNotify(T time)
        {
            if (!this.inputImmutable1)
                foreach (var record in this.Input1.GetRecordsAt(time))
                    OnInput1(record, time);

            if (!this.inputImmutable2)
                foreach (var record in this.Input2.GetRecordsAt(time))
                    OnInput2(record, time);

            Compute(time);
            Flush();

            if (inputTrace1 != null) inputTrace1.Compact();
            if (inputTrace2 != null) inputTrace2.Compact();
        }

        protected Dictionary<K, BinaryKeyIndices> keyIndices;
        protected Dictionary<K, BinaryKeyIndices> curKeyIndices;

        protected CollectionTraceCheckpointable<V1> inputTrace1;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<V2> inputTrace2;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<R> outputTrace;             // collects outputs

        protected int outputWorkspace = 0;

        protected LatticeInternTable<T> internTable = new LatticeInternTable<T>();

        //protected NaiadList<K> keysToProcess = new NaiadList<K>(1);

        protected Dictionary<T, HashSet<K>> KeysToProcessAtTimes = new Dictionary<T, HashSet<K>>();

        public virtual void OnInput1(Weighted<S1> entry, T time)
        {
            var k = key1(entry.record);

            BinaryKeyIndices state;
            if (!keyIndices.TryGetValue(k, out state))
                state = new BinaryKeyIndices();

            if (state.unprocessed1 == 0 && state.unprocessed2 == 0)
            {
                if (!this.KeysToProcessAtTimes.ContainsKey(time))
                    this.KeysToProcessAtTimes.Add(time, new HashSet<K>());

                this.KeysToProcessAtTimes[time].Add(k);
            }

            inputTrace1.Introduce(ref state.unprocessed1, value1(entry.record), entry.weight, internTable.Intern(time));

            keyIndices[k] = state;
            curKeyIndices[k] = state;
        }

        public virtual void OnInput2(Weighted<S2> entry, T time)
        {
            var k = key2(entry.record);

            BinaryKeyIndices state;
            if (!keyIndices.TryGetValue(k, out state))
                state = new BinaryKeyIndices();

            if (state.unprocessed1 == 0 && state.unprocessed2 == 0)
            {
                if (!this.KeysToProcessAtTimes.ContainsKey(time))
                    this.KeysToProcessAtTimes.Add(time, new HashSet<K>());

                this.KeysToProcessAtTimes[time].Add(k);
            }

            inputTrace2.Introduce(ref state.unprocessed2, value2(entry.record), entry.weight, internTable.Intern(time));

            keyIndices[k] = state;
            curKeyIndices[k] = state;
        }

        public virtual void Compute(T time)
        {
            if (this.KeysToProcessAtTimes.ContainsKey(time))
            {
                foreach (var key in this.KeysToProcessAtTimes[time])
                    Update(key, time);

                this.KeysToProcessAtTimes.Remove(time);

                inputTrace1.Compact();
                inputTrace2.Compact();
                outputTrace.Compact();
            }
        }

        protected NaiadList<Weighted<V1>> collection1 = new NaiadList<Weighted<V1>>(1);
        protected NaiadList<Weighted<V1>> difference1 = new NaiadList<Weighted<V1>>(1);

        protected NaiadList<Weighted<V2>> collection2 = new NaiadList<Weighted<V2>>(1);
        protected NaiadList<Weighted<V2>> difference2 = new NaiadList<Weighted<V2>>(1);


        // Moves from unprocessed[key] to processed[key], updating output[key] and Send()ing.
        protected virtual void Update(K key, T time)
        {
            var traceIndices = new BinaryKeyIndices();
            bool present = this.keyIndices.TryGetValue(key, out traceIndices);

            if (present)
            {
                if (traceIndices.unprocessed1 != 0 || traceIndices.unprocessed2 != 0)
                {
                    // iterate through the times that may require updates.
                    var interestingTimes = InterestingTimes(traceIndices);
                    for (int i = 0; i < interestingTimes.Count; i++)
                    {
                        var newTime = this.internTable.times[interestingTimes.Array[i]];
                        if (!newTime.Equals(time))
                        {
                            if (!this.KeysToProcessAtTimes.ContainsKey(newTime))
                            {
                                this.KeysToProcessAtTimes.Add(newTime, new HashSet<K>());
                                this.NotifyAt(newTime);
                            }

                            this.KeysToProcessAtTimes[newTime].Add(key);
                        }
                    }

                    // move the differences we produced from local to persistent storage.
                    outputTrace.IntroduceFrom(ref traceIndices.output, ref outputWorkspace);

                    // incorporate the updates, so we can compare old and new outputs.
                    inputTrace1.IntroduceFrom(ref traceIndices.processed1, ref traceIndices.unprocessed1, true);
                    inputTrace2.IntroduceFrom(ref traceIndices.processed2, ref traceIndices.unprocessed2, true);
                }

                inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.processed1);
                inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.processed2);
                outputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.output);

                UpdateTime(key, traceIndices, this.internTable.Intern(time));

                outputTrace.IntroduceFrom(ref traceIndices.output, ref this.outputWorkspace, true);

                keyIndices[key] = traceIndices;
                curKeyIndices[key] = traceIndices;
            }
        }

        protected NaiadList<int> timeList = new NaiadList<int>(1);
        protected NaiadList<int> truthList = new NaiadList<int>(1);
        protected NaiadList<int> deltaList = new NaiadList<int>(1);

        protected virtual NaiadList<int> InterestingTimes(BinaryKeyIndices keyIndex)
        {
            deltaList.Clear();
            inputTrace1.EnumerateTimes(keyIndex.unprocessed1, deltaList);
            inputTrace2.EnumerateTimes(keyIndex.unprocessed2, deltaList);

            truthList.Clear();
            inputTrace1.EnumerateTimes(keyIndex.processed1, truthList);
            inputTrace2.EnumerateTimes(keyIndex.processed2, truthList);

            timeList.Clear();
            this.internTable.InterestingTimes(timeList, truthList, deltaList);

            return timeList;
        }

        protected NaiadList<Weighted<R>> outputCollection = new NaiadList<Weighted<R>>(1);

        protected virtual void UpdateTime(K key, BinaryKeyIndices keyIndex, int timeIndex)
        {
            // subtract out prior records before adding new ones
            // outputTrace.SubtractStrictlyPriorDifferences(ref outputWorkspace, timeIndex);

            NewOutputMinusOldOutput(key, keyIndex, timeIndex);

            var outputTime = this.internTable.times[timeIndex];

            outputCollection.Clear();
            outputTrace.EnumerateDifferenceAt(outputWorkspace, timeIndex, outputCollection);

            var output = this.Output.GetBufferForTime(outputTime);

            for (int i = 0; i < outputCollection.Count; i++)
                output.Send(outputCollection.Array[i]);
        }

        protected virtual void NewOutputMinusOldOutput(K key, BinaryKeyIndices keyIndex, int timeIndex)
        {
            if (keyIndex.processed1 != 0 || keyIndex.processed2 != 0)
                Reduce(key, keyIndex, timeIndex);

            // this suggests we want to init updateToOutput with -output
            outputCollection.Clear();
            outputTrace.EnumerateCollectionAt(keyIndex.output, timeIndex, outputCollection);
            for (int i = 0; i < outputCollection.Count; i++)
                outputTrace.Introduce(ref outputWorkspace, outputCollection.Array[i].record, -outputCollection.Array[i].weight, timeIndex);
        }

        // expected to populate resultList to match reduction(collection.source)
        protected virtual void Reduce(K key, BinaryKeyIndices keyIndex, int time) { }

        /* Checkpoint format:
         * bool terminated
         * if !terminated:
         *     LatticeInternTable<T>                            internTable
         *     CollectionTrace<>                                inputTrace1
         *     CollectionTrace<>                                inputTrace2
         *     CollectionTrace<>                                outputTrace
         *     Dictionary<K,KeyIndices>                         keysToProcess
         *     int                                              recordsToProcessCount1
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess1
         *     int                                              recordsToProcessCount2
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess2
         */

        protected override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                this.internTable.Checkpoint(writer);
                this.inputTrace1.Checkpoint(writer);
                this.inputTrace2.Checkpoint(writer);
                this.outputTrace.Checkpoint(writer);

                this.keyIndices.Checkpoint(writer);

                this.Input1.Checkpoint(writer);
                this.Input2.Checkpoint(writer);

                /*
                writer.Write(this.recordsToProcess1.Count, PrimitiveSerializers.Int32);
                foreach (KeyValuePair<T, NaiadList<Weighted<S1>>> kvp in this.recordsToProcess1)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, weightedS1Serializer);
                }

                writer.Write(this.recordsToProcess2.Count, PrimitiveSerializers.Int32);
                foreach (KeyValuePair<T, NaiadList<Weighted<S2>>> kvp in this.recordsToProcess2)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, weightedS2Serializer);
                }
                 */
            }
        }

        protected override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                this.internTable.Restore(reader);
                this.inputTrace1.Restore(reader);
                this.inputTrace2.Restore(reader);
                this.outputTrace.Restore(reader);

                this.keyIndices.Restore(reader);

                this.Input1.Restore(reader);
                this.Input2.Restore(reader);

                /*
                int recordsToProcessCount1 = reader.Read<int>(PrimitiveSerializers.Int32);

                foreach (NaiadList<Weighted<S1>> recordList in this.recordsToProcess1.Values)
                    recordList.Free();
                this.recordsToProcess1.Clear();

                for (int i = 0; i < recordsToProcessCount1; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    NaiadList<Weighted<S1>> value = new NaiadList<Weighted<S1>>();
                    value.Restore(reader, weightedS1Serializer);
                    this.recordsToProcess1[key] = value;
                }

                int recordsToProcessCount2 = reader.Read<int>(PrimitiveSerializers.Int32);

                foreach (NaiadList<Weighted<S2>> recordList in this.recordsToProcess2.Values)
                    recordList.Free();
                this.recordsToProcess2.Clear();

                for (int i = 0; i < recordsToProcessCount2; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    NaiadList<Weighted<S2>> value = new NaiadList<Weighted<S2>>();
                    value.Restore(reader, weightedS2Serializer);
                    this.recordsToProcess2[key] = value;
                }
                 */
            }
        }

        // private bool HasStateInCheckpoint(BinaryKeyIndices indices, ICheckpoint<T> checkpoint)
        // {
        //     // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

        //     NaiadList<int> timeList = new NaiadList<int>(16);
        //     this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
        //     for (int i = 0; i < timeList.Count; ++i)
        //     {
        //         if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
        //         {
        //             return true;
        //         }
        //     }

        //     timeList.Clear();
        //     this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
        //     for (int i = 0; i < timeList.Count; ++i)
        //     {
        //         if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
        //         {
        //             return true;
        //         }
        //     }

        //     timeList.Clear();
        //     this.outputTrace.EnumerateTimes(indices.output, timeList);
        //     for (int i = 0; i < timeList.Count; ++i)
        //     {
        //         if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
        //         {
        //             return true;
        //         }
        //     }

        //     return false;
        // }

        private Pair<Pair<int, int>, int> CountTimesInCheckpoint(BinaryKeyIndices indices, ICheckpoint<T> checkpoint)
        {
            // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

            NaiadList<int> timeList = new NaiadList<int>(16);
            this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
            int input1Count = 0;
            for (int i = 0; i < timeList.Count; ++i)
            {
                if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                {
                    ++input1Count;
                }
            }

            timeList.Clear();
            this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
            int input2Count = 0;
            for (int i = 0; i < timeList.Count; ++i)
            {
                if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                {
                    ++input2Count;
                }
            }

            timeList.Clear();
            this.outputTrace.EnumerateTimes(indices.output, timeList);
            int outputCount = 0;
            for (int i = 0; i < timeList.Count; ++i)
            {
                if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                {
                    ++outputCount;
                }
            }

            return input1Count.PairWith(input2Count).PairWith(outputCount);
        }

        private readonly CheckpointHelpers.IncrementalCheckpointManager<T> checkpointManager;

        protected override bool NextCheckpointIsIncremental(ICheckpoint<T> candidateIncrementalCheckpoint)
        {
            if (this.isShutdown || this.checkpointManager.ForceFullCheckpoint)
            {
                return false;
            }

            long entries = this.CountEntries(candidateIncrementalCheckpoint);

            return this.checkpointManager.NextCheckpointIsIncremental(entries);
        }

        protected virtual long CountEntries(ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            if (!this.isShutdown)
            {
                foreach (var indices in this.curKeyIndices.Values)
                {
                    checkpointEntries +=
                        this.inputTrace1.CountEntries(indices.processed1, checkpoint, this.internTable.times, true, false).First;
                    checkpointEntries +=
                        this.inputTrace2.CountEntries(indices.processed2, checkpoint, this.internTable.times, true, false).First;
                    checkpointEntries +=
                        this.outputTrace.CountEntries(indices.output, checkpoint, this.internTable.times, true, false).First;
                }
            }

            return checkpointEntries;
        }

        protected override void Checkpoint(NaiadWriter writer, ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                if (checkpoint.IsFullCheckpoint)
                {
                    this.CompactInternTable();
                    this.curKeyIndices = keyIndices;
                }

                List<Pair<Pair<K, BinaryKeyIndices>, Pair<Pair<int, int>, int>>> indicesTimeCounts =
                  new List<Pair<Pair<K, BinaryKeyIndices>, Pair<Pair<int, int>, int>>>();

                foreach (var key in this.curKeyIndices)
                {
                  BinaryKeyIndices indices = key.Value;
                  Pair<Pair<int, int>, int> timeCount =
                    this.CountTimesInCheckpoint(indices, checkpoint);
                  if (timeCount.First.First > 0 ||
                      timeCount.First.Second > 0 ||
                      timeCount.Second > 0)
                  {
                    indicesTimeCounts.Add((key.Key.PairWith(indices)).PairWith(timeCount));
                  }
                }
                writer.Write(indicesTimeCounts.Count);
                foreach (var indicesTimeCount in indicesTimeCounts)
                {
                  writer.Write(indicesTimeCount.First.First);
                  writer.Write(indicesTimeCount.Second.First.First);
                  checkpointEntries +=
                    this.inputTrace1.CheckpointKey(indicesTimeCount.First.Second.processed1,
                                                   checkpoint,
                                                   this.internTable.times,
                                                   writer);
                  writer.Write(indicesTimeCount.Second.First.Second);
                  checkpointEntries +=
                    this.inputTrace2.CheckpointKey(indicesTimeCount.First.Second.processed2,
                                                   checkpoint,
                                                   this.internTable.times,
                                                   writer);
                  writer.Write(indicesTimeCount.Second.Second);
                  checkpointEntries +=
                    this.outputTrace.CheckpointKey(indicesTimeCount.First.Second.output,
                                                   checkpoint,
                                                   this.internTable.times,
                                                   writer);
                }

                // int compactedCount = 0;
                // foreach (var indices in this.keyIndices.Values)
                // {
                //     if (this.HasStateInCheckpoint(indices, checkpoint))
                //     {
                //         ++compactedCount;
                //     }
                // }

                // writer.Write(compactedCount);
                // foreach (var key in this.keyIndices)
                // {
                //     BinaryKeyIndices indices = key.Value;

                //     Pair<Pair<int, int>, int> timeCounts = this.CountTimesInCheckpoint(indices, checkpoint);

                //     if (timeCounts.First.First > 0 || timeCounts.First.Second > 0 || timeCounts.Second > 0)
                //     {
                //         writer.Write(key.Key);

                //         writer.Write(timeCounts.First.First);
                //         checkpointEntries +=
                //             this.inputTrace1.CheckpointKey(indices.processed1, checkpoint, this.internTable.times, writer);

                //         writer.Write(timeCounts.First.Second);
                //         checkpointEntries +=
                //             this.inputTrace2.CheckpointKey(indices.processed2, checkpoint, this.internTable.times, writer);

                //         writer.Write(timeCounts.Second);
                //         checkpointEntries +=
                //             this.outputTrace.CheckpointKey(indices.output, checkpoint, this.internTable.times, writer);
                //     }
                // }

                writer.Write(this.KeysToProcessAtTimes.Count);
                foreach (var time in this.KeysToProcessAtTimes)
                {
                    writer.Write(time.Key);
                    writer.Write(time.Value.Count);
                    foreach (var key in time.Value)
                    {
                        writer.Write(key);
                    }
                }
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        protected override bool CanRollBackPreservingState(Pointstamp[] frontier)
        {
            // we are careful not to coalesce times in the potential rollback range, so we are able to roll back without
            // restoring from a checkpoint.
            return true;
        }

        public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<T> lastFullCheckpoint, ICheckpoint<T> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            if (!this.isShutdown)
            {
                bool countFull = !this.checkpointManager.CachedFullCheckpoint.Equals(lastFullCheckpoint);
                bool countIncremental = countFull || !this.checkpointManager.CachedIncrementalCheckpoint.Equals(lastIncrementalCheckpoint);

                Pair<long, long> counts = 0L.PairWith(0L);

                if (frontier.Length == 0)
                {
                    this.InitializeRestoration(frontier);
                }
                else
                {
                    ICheckpoint<T> timeRange = FrontierCheckpointTester<T>.CreateDownwardClosed(frontier);

                    foreach (var key in this.keyIndices.Keys.ToArray())
                    {
                        BinaryKeyIndices indices = this.keyIndices[key];

                        this.inputTrace1.RemoveStateInTimes(ref indices.processed1, t => timeRange.ContainsTime(this.internTable.times[t]));
                        this.inputTrace1.ZeroState(ref indices.unprocessed1);
                        this.inputTrace2.RemoveStateInTimes(ref indices.processed2, t => timeRange.ContainsTime(this.internTable.times[t]));
                        this.inputTrace2.ZeroState(ref indices.unprocessed2);
                        this.outputTrace.RemoveStateInTimes(ref indices.output, t => timeRange.ContainsTime(this.internTable.times[t]));

                        if (countFull || countIncremental)
                        {
                            Pair<long, long> thisCounts;
                            thisCounts = this.inputTrace1.CountEntries(indices.processed1, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                            thisCounts = this.inputTrace2.CountEntries(indices.processed2, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                            thisCounts = this.outputTrace.CountEntries(indices.output, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                        }

                        this.keyIndices[key] = indices;
                    }

                    if (!countFull)
                    {
                        counts.First = this.checkpointManager.CachedFullCheckpointEntries;
                    }

                    if (!countIncremental)
                    {
                        counts.Second = this.checkpointManager.CachedIncrementalCheckpointEntries;
                    }

                    this.checkpointManager.RegisterCheckpoint(counts.First, lastFullCheckpoint, counts.Second, lastIncrementalCheckpoint);
                }
                this.curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            }
        }

        internal virtual void CompactInternTable()
        {
            if (!this.isShutdown)
            {
                LatticeInternTable<T> newInternTable = new LatticeInternTable<T>();
                bool[] usedTimes = new bool[this.internTable.count];

                foreach (var key in this.keyIndices.Keys.ToArray())
                {
                    BinaryKeyIndices indices = this.keyIndices[key];
                    this.inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed1);
                    this.inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.unprocessed1);
                    this.inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed2);
                    this.inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.unprocessed2);
                    this.outputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.output);
                    if (indices.IsEmpty)
                    {
                        this.keyIndices.Remove(key);
                        this.curKeyIndices.Remove(key);
                    }
                    else
                    {
                        this.keyIndices[key] = indices;
                        if (this.curKeyIndices.ContainsKey(key))
                          this.curKeyIndices[key] = indices;

                        this.inputTrace1.MarkUsedTimes(indices.processed1, usedTimes);
                        this.inputTrace1.MarkUsedTimes(indices.unprocessed1, usedTimes);
                        this.inputTrace2.MarkUsedTimes(indices.processed2, usedTimes);
                        this.inputTrace2.MarkUsedTimes(indices.unprocessed2, usedTimes);
                        this.outputTrace.MarkUsedTimes(indices.output, usedTimes);
                    }
                }

                int[] transferTime = new int[this.internTable.count];
                for (int i=0; i<this.internTable.count; ++i)
                {
                    if (usedTimes[i])
                    {
                        transferTime[i] = newInternTable.Intern(this.internTable.times[i]);
                    }
                }

                foreach (var indices in this.keyIndices.Values)
                {
                    this.inputTrace1.TransferTimesToNewInternTable(indices.processed1, transferTime);
                    this.inputTrace1.TransferTimesToNewInternTable(indices.unprocessed1, transferTime);
                    this.inputTrace2.TransferTimesToNewInternTable(indices.processed2, transferTime);
                    this.inputTrace2.TransferTimesToNewInternTable(indices.unprocessed2, transferTime);
                    this.outputTrace.TransferTimesToNewInternTable(indices.output, transferTime);
                }

                this.inputTrace1.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.inputTrace2.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.outputTrace.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.internTable = newInternTable;
            }
        }

        protected override void InitializeRestoration(Pointstamp[] frontier)
        {
            // empty all the state
            this.keyIndices = new Dictionary<K, BinaryKeyIndices>();
            this.curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            this.internTable = new LatticeInternTable<T>();
            this.inputTrace1 = createInputTrace1();
            this.inputTrace2 = createInputTrace2();
            this.outputTrace = createOutputTrace();
            this.checkpointManager.ForceFullCheckpoint = true;
        }

        protected override void RestorePartialCheckpoint(NaiadReader reader, ICheckpoint<T> checkpoint)
        {
            this.curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            long checkpointEntries = 0;

            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                int numberOfKeys = reader.Read<int>();
                for (int i = 0; i < numberOfKeys; ++i)
                {
                    K key = reader.Read<K>();

                    BinaryKeyIndices indices;
                    if (!this.keyIndices.TryGetValue(key, out indices))
                    {
                        indices = new BinaryKeyIndices();
                    }

                    checkpointEntries += this.inputTrace1.RestoreKey(ref indices.processed1, this.internTable, reader);
                    checkpointEntries += this.inputTrace2.RestoreKey(ref indices.processed2, this.internTable, reader);
                    checkpointEntries += this.outputTrace.RestoreKey(ref indices.output, this.internTable, reader);

                    this.keyIndices[key] = indices;
                    this.curKeyIndices[key] = indices;
                }

                int timeCount = reader.Read<int>();
                for (int i = 0; i < timeCount; ++i)
                {
                    T time = reader.Read<T>();
                    HashSet<K> keys;
                    if (!this.KeysToProcessAtTimes.TryGetValue(time, out keys))
                    {
                        keys = new HashSet<K>();
                        this.KeysToProcessAtTimes.Add(time, keys);
                    }
                    int keyCount = reader.Read<int>();
                    for (int k = 0; k < keyCount; ++k)
                    {
                        keys.Add(reader.Read<K>());
                    }
                }
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        protected readonly bool inputImmutable1 = false;
        protected readonly bool inputImmutable2 = false;

        public ConservativeBinaryStatefulOperator(int index, Stage<T> stage, bool input1Immutable, bool input2Immutable, Expression<Func<S1, K>> k1, Expression<Func<S2, K>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, bool maintainOC = true)
            : base(index, stage, null)
        {
            key1 = k1.Compile();
            value1 = v1.Compile();

            key2 = k2.Compile();
            value2 = v2.Compile();

            keyExpression1 = k1;
            keyExpression2 = k2;
            valueExpression1 = v1;
            valueExpression2 = v2;

            inputImmutable1 = input1Immutable;
            inputImmutable2 = input2Immutable;

            keyIndices = new Dictionary<K, BinaryKeyIndices>();
            curKeyIndices = new Dictionary<K, BinaryKeyIndices>();
            inputTrace1 = createInputTrace1();
            inputTrace2 = createInputTrace2();
            outputTrace = createOutputTrace();

            outputWorkspace = 0;

            this.checkpointManager = new CheckpointHelpers.IncrementalCheckpointManager<T>(this.Stage);
        }
    }

    internal abstract class BinaryStatefulOperator<S, T> : OperatorImplementations.BinaryStatefulOperator<S, S, S, S, S, T, S>
        where S : IEquatable<S>
        where T : Time<T>
    {
        protected abstract Int64 WeightFunction(Int64 weight1, Int64 weight2);

        protected override void NewOutputMinusOldOutput(S key, BinaryKeyIndices keyIndices, int timeIndex)
        {
            collection1.Clear();
            inputTrace1.EnumerateCollectionAt(keyIndices.processed1, timeIndex, collection1);

            collection2.Clear();
            inputTrace2.EnumerateCollectionAt(keyIndices.processed2, timeIndex, collection2);

            var newSum1 = 0L;
            for (int i = 0; i < collection1.Count; i++)
                newSum1 += collection1.Array[i].weight;

            var newSum2 = 0L;
            for (int i = 0; i < collection2.Count; i++)
                newSum2 += collection2.Array[i].weight;

            difference1.Clear();
            inputTrace1.EnumerateCollectionAt(keyIndices.unprocessed1, timeIndex, difference1);

            difference2.Clear();
            inputTrace2.EnumerateCollectionAt(keyIndices.unprocessed2, timeIndex, difference2);

            var oldSum1 = newSum1;
            for (int i = 0; i < difference1.Count; i++)
                oldSum1 -= difference1.Array[i].weight;

            var oldSum2 = newSum2;
            for (int i = 0; i < difference2.Count; i++)
                oldSum2 -= difference2.Array[i].weight;

            var oldOut = WeightFunction(oldSum1, oldSum2);
            var newOut = WeightFunction(newSum1, newSum2);

            if (oldOut != newOut)
                outputTrace.Introduce(ref outputWorkspace, key, newOut - oldOut, timeIndex);
        }

        public BinaryStatefulOperator(int index, Stage<T> stage, bool input1Immutable, bool input2Immutable)
            : base(index, stage, input1Immutable, input2Immutable, x => x, x => x, x => x, x => x, false)
        {
        }
    }
}
