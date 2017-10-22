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
using System.Threading.Tasks;
using Microsoft.Research.Naiad.DataStructures;
using System.Diagnostics;
using Microsoft.Research.Naiad.Dataflow.Channels;
using System.Linq.Expressions;
using Microsoft.Research.Naiad.Serialization;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using System.IO;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.OperatorImplementations;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;

using StackExchange.Redis;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    internal static class ExtensionHelpers
    {
        private static List<Pair<Expression, Expression>> expressionMapping = new List<Pair<Expression, Expression>>();

        public static Expression ReverseLookUp(this Expression target)
        {
            for (int i = 0; i < expressionMapping.Count; i++)
                if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(target, expressionMapping[i].Second))
                    return expressionMapping[i].First;

            return null;
        }

        public static Expression LookUp(this Expression source)
        {
            for (int i = 0; i < expressionMapping.Count; i++)
                if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(source, expressionMapping[i].First))
                    return expressionMapping[i].Second;

            return null;
        }

        public static Expression LookUpOrAdd(this Expression source, Expression alternate)
        {
            for (int i = 0; i < expressionMapping.Count; i++)
                if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(source, expressionMapping[i].First))
                    return expressionMapping[i].Second;

            expressionMapping.Add(new Pair<Expression, Expression>(source, alternate));
            return alternate;
        }

        public static Expression<Func<Weighted<S>, int>> ConvertToWeightedFuncAndHashCode<S, K>(this Expression<Func<S, K>> func)
            where S : IEquatable<S>
        {
            if (func == null)
                return null;

            var compiled = func.Compile();
            Expression<Func<Weighted<S>, int>> result = x => compiled(x.record).GetHashCode();

            return LookUpOrAdd(func, result) as Expression<Func<Weighted<S>, int>>;
        }

        internal static DataflowCollection<R, T> ToCollection<R, T>(this Stream<Weighted<R>, T> stream)
            where T : Time<T>
            where R : IEquatable<R>
        {
            return new DataflowCollection<R, T>(stream);
        }
        public static Collection<R, T> ToCollection<R, T>(this Stream<Weighted<R>, T> stream, bool immutable)
            where T : Time<T>
            where R : IEquatable<R>
        {
            var result = new DataflowCollection<R, T>(stream);
            result.immutable = immutable;
            return result;
        }
    }

    internal abstract class TypedCollection<R, T> : Collection<R, T>
        where R : IEquatable<R>
        where T : Time<T>
    {
        internal bool immutable = false;
        internal bool Immutable { get { return immutable; } }

        public abstract Stream<Weighted<R>, T> Output { get; }

        public Collection<R, T> SetCheckpointType(CheckpointType checkpointType)
        {
            this.Output.SetCheckpointType(checkpointType);
            return this;
        }

        public Collection<R, T> SetCheckpointPolicy(Func<int, Runtime.FaultTolerance.ICheckpointPolicy> shouldCheckpointFactory)
        {
            this.Output.SetCheckpointPolicy(shouldCheckpointFactory);
            return this;
        }

        public static Expression<Func<Weighted<S>, int>> IdentityWeightedFuncAndHashCode<S>()
            where S : IEquatable<S>
        {
            Expression<Func<S, S>> func = x => x;

            var compiled = func.Compile();
            Expression<Func<Weighted<S>, int>> result = x => compiled(x.record).GetHashCode();

            return ExtensionHelpers.LookUpOrAdd(func, result) as Expression<Func<Weighted<S>, int>>;
        }

        internal virtual Expression OutputPartitionedBy
        {
            get { return this.Output.PartitionedBy.ReverseLookUp(); }
        }

        #region Naiad Operators

        public Collection<R, T> PartitionBy<K>(Expression<Func<R, K>> partitionFunction)
        {
            var stream = Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(this.Output, partitionFunction.ConvertToWeightedFuncAndHashCode());

            return stream.ToCollection();
        }

        public Collection<R, T> ForcePartitionBy<K>(Expression<Func<R, K>> partitionFunction)
        {
            var stream = Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.ForcePartitionBy(this.Output, partitionFunction.ConvertToWeightedFuncAndHashCode());

            return stream.ToCollection();
        }

        #region Consolidation

        public Collection<R, T> Consolidate<K>(Expression<Func<R, K>> partitionFunction)
        {

            if (partitionFunction == null)  // we still assume you want consolidation, even if you can't think of a good function.
                return this.Consolidate();
            else
                return this.Consolidate(partitionFunction.ConvertToWeightedFuncAndHashCode());
            }

        public Collection<R, T> Consolidate()
        {
            if (this.Output.PartitionedBy != null)
                return this.Consolidate(this.Output.PartitionedBy);
            else
                return this.Consolidate(x => x.record.GetHashCode());
        }

        internal Collection<R, T> Consolidate(Expression<Func<Weighted<R>,int>> partitionFunction)
        {
            Collection<R, T> consolidated = this.Manufacture((i, v) => new Operators.Consolidate<R, T>(i, v), partitionFunction, partitionFunction, false, "Consolidate");
            return consolidated;
        }

        #endregion Consolidation

        #region Lattice adjustment

        public Collection<R, T> AdjustTime(Func<R, T, T> adjustment)
        {
            if (adjustment == null)
                throw new ArgumentNullException("adjustment");

            var result = this.Manufacture((i, v) => new Operators.AdjustTime<R, T>(i, v, adjustment), this.Output.PartitionedBy, this.Output.PartitionedBy, false, "AdjustLattice");

            result.immutable = false;

            // manufacture sets the checkpoint type to StatelessDiscardAll but since we are adjusting the times we actually
            // have to keep track of which times are being discarded
            return result.SetCheckpointType(CheckpointType.Stateless);
        }

        #endregion Lattice adjustment

        private InternalCollection<R2, T> Manufacture<R2>(Func<int, Stage<T>, UnaryVertex<Weighted<R>, Weighted<R2>, T>> factory, Expression<Func<Weighted<R>, int>> inputPartitionedBy, Expression<Func<Weighted<R2>, int>> outputPartitionedBy, bool stateful, string name)
            where R2 : IEquatable<R2>
        {
            var output = Foundry.NewUnaryStage(this.Output, factory, inputPartitionedBy, outputPartitionedBy, name);
            if (stateful)
            {
                output.SetCheckpointType(Runtime.FaultTolerance.CheckpointType.Stateful);
            }
            else
            {
                output.SetCheckpointType(Runtime.FaultTolerance.CheckpointType.Stateless);
            }
            
            return new InternalCollection<R2, T>(output, this.Immutable);
        }

        private InternalCollection<R2, T> Manufacture<S, R2>(TypedCollection<S, T> other, Func<int, Stage<T>, BinaryVertex<Weighted<R>, Weighted<S>, Weighted<R2>, T>> factory, Expression<Func<Weighted<R>, int>> input1PartitionedBy, Expression<Func<Weighted<S>, int>> input2PartitionedBy, Expression<Func<Weighted<R2>, int>> outputPartitionedBy, bool stateful, string name)
            where S : IEquatable<S>
            where R2 : IEquatable<R2>
        {
            var output = Foundry.NewBinaryStage(this.Output, other.Output, factory, input1PartitionedBy, input2PartitionedBy, outputPartitionedBy, name);
            if (stateful)
            {
                output.SetCheckpointType(Runtime.FaultTolerance.CheckpointType.Stateful);
            }
            else
            {
                output.SetCheckpointType(Runtime.FaultTolerance.CheckpointType.Stateless);
            }

            return new InternalCollection<R2, T>(output, this.Immutable && other.Immutable);
        }


        #region Select/Where/SelectMany

        public Collection<R2, T> Select<R2>(Expression<Func<R, R2>> selector)
            where R2 : IEquatable<R2>
        {
            if (selector == null)
                throw new ArgumentNullException("selector");

            return this.Manufacture<R2>((i, v) => new Operators.Select<R, T, R2>(i, v, selector), null, null, false, "Select");
        }

        public Collection<R, T> Print(string name, System.Diagnostics.Stopwatch stopwatch)
        {
          return this.Manufacture<R>((i, v) => new Operators.Print<R, T>(i, v, name, stopwatch), null, null, false, "Print");
        }

        public Collection<R, T> Where(Expression<Func<R, bool>> predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException("predicate");

            return this.Manufacture((i, v) => new Operators.Where<R, T>(i, v, predicate), this.Output.PartitionedBy, this.Output.PartitionedBy, false, "Where");
        }

        public Collection<R2, T> SelectMany<R2>(Expression<Func<R, IEnumerable<R2>>> selector)
            where R2 : IEquatable<R2>
        {
            if (selector == null)
                throw new ArgumentNullException("selector");

            return this.Manufacture<R2>((i, v) => new Operators.SelectMany<R, T, R2>(i, v, selector), null, null, false, "SelectMany");
        }

        public Collection<R2, T> SelectMany<R2>(Expression<Func<R, IEnumerable<ArraySegment<R2>>>> selector)
            where R2 : IEquatable<R2>
        {
            if (selector == null)
                throw new ArgumentNullException("selector");

            return this.Manufacture<R2>((i, v) => new Operators.SelectManyBatch<R, T, R2>(i, v, selector), null, null, false, "SelectManyBatch");
        }

        #endregion Select/Where/SelectMany

        #region YCSB
      public Collection<R2, T> AdEventGenerator<R2>(Expression<Func<string, string, R2>> resultFunc,
                                                    string[] preparedAds,
                                                    long numEventsPerEpoch)
          where R2: IEquatable<R2>
      {
          if (resultFunc == null)
            throw new ArgumentNullException("resultFunc");

          return this.Manufacture<R2>((i, v) => new Operators.AdEventGeneratorVertex<R, T, R2>(i, v, resultFunc, preparedAds, numEventsPerEpoch), null, null, false, "AdEventGenerator");
      }

        public Collection<R2, T> RedisCampaign<R2>(Expression<Func<R, string>> adFunc,
                                                   Expression<Func<R, string>> timeFunc,
                                                   Expression<Func<string, string, R2>> resultFunc,
                                                   ConnectionMultiplexer redis,
                                                   Dictionary<string, string> adsToCampaign)
          where R2: IEquatable<R2>
        {
          if (adFunc == null)
            throw new ArgumentNullException("adFunc");
          if (timeFunc == null)
            throw new ArgumentNullException("timeFunc");
          if (resultFunc == null)
            throw new ArgumentNullException("resultFunc");

          return this.Manufacture<R2>((i, v) => new Operators.RedisCampaignVertex<R, T, R2>(i, v, adFunc, timeFunc, resultFunc, redis, adsToCampaign), null, null, false, "RedisCampaign");
        }

        public Collection<R2, T> RedisCampaign2<R2>(Expression<Func<R, string>> adFunc,
                                                    Expression<Func<string, string, R2>> resultFunc,
                                                    Dictionary<string, string> adsToCampaign)
          where R2: IEquatable<R2>
        {
          if (resultFunc == null)
            throw new ArgumentNullException("resultFunc");

          return this.Manufacture<R2>((i, v) => new Operators.RedisCampaignVertex2<R, T, R2>(i, v, adFunc, resultFunc, adsToCampaign), null, null, false, "RedisCampaign2");
        }

        public Collection<R2, T> RedisCampaignProcessor<R2>(Expression<Func<R, string>> campaignFunc,
                                                            Expression<Func<R, long>> timeFunc,
                                                            Expression<Func<R, long>> countFunc,
                                                            ConnectionMultiplexer redis)
          where R2: IEquatable<R2>
        {
          if (campaignFunc == null)
            throw new ArgumentNullException("campaignFunc");
          if (timeFunc == null)
            throw new ArgumentNullException("timeFunc");
          if (countFunc == null)
            throw new ArgumentNullException("countFunc");

          return this.Manufacture<R2>((i, v) => new Operators.RedisCampaignProcessorVertex<R, T, R2>(i, v, campaignFunc, timeFunc, countFunc, redis), null, null, false, "RedisCampaignProcessor");
        }

        #endregion YCSB

        #region GroupBy/CoGroupBy

        public Collection<R2, T> GroupBy<K, V, R2>(Expression<Func<R, K>> key, Expression<Func<R, V>> selector, Func<K, IEnumerable<V>, IEnumerable<R2>> reducer)
            where K : IEquatable<K>
            where V : IEquatable<V>
            where R2 : IEquatable<R2>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (selector == null)
                throw new ArgumentNullException("selector");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i, v) => new Operators.GroupBy<K, V, R, T, R2>(i, v, this.Immutable, key, selector, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "SelectMany");
        }

        public Collection<R2, T> GroupBy<K, R2>(Expression<Func<R, K>> key, Func<K, IEnumerable<R>, IEnumerable<R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
        {
            return this.GroupBy(key, x => x, reducer);
        }

        public Collection<R3, T> CoGroupBy<K, V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, V1>> selector1, Expression<Func<R2, V2>> selector2, Expression<Func<K, IEnumerable<V1>, IEnumerable<V2>, IEnumerable<R3>>> reducer)
            where K : IEquatable<K>
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (selector1 == null)
                throw new ArgumentNullException("selector1");
            if (selector2 == null)
                throw new ArgumentNullException("selector2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var that = other as TypedCollection<R2, T>;

            return this.Manufacture<R2, R3>(that, (i, v) => new Operators.CoGroupBy<K, V1, V2, R, R2, T, R3>(i, v, this.immutable, that.immutable, key1, key2, selector1, selector2, reducer), key1.ConvertToWeightedFuncAndHashCode(), key2.ConvertToWeightedFuncAndHashCode(), null, true, "CoGroupBy");
        }

        public Collection<R3, T> CoGroupBy<K, V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, V1>> selector1, Expression<Func<R2, V2>> selector2, Expression<Func<K, IEnumerable<Weighted<V1>>, IEnumerable<Weighted<V2>>, IEnumerable<Weighted<R3>>>> reducer)
            where K : IEquatable<K>
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (selector1 == null)
                throw new ArgumentNullException("selector1");
            if (selector2 == null)
                throw new ArgumentNullException("selector2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var that = other as TypedCollection<R2, T>;

            return this.Manufacture<R2, R3>(that, (i, v) => new Operators.CoGroupBy<K, V1, V2, R, R2, T, R3>(i, v, this.immutable, that.immutable, key1, key2, selector1, selector2, reducer), key1.ConvertToWeightedFuncAndHashCode(), key2.ConvertToWeightedFuncAndHashCode(), null, true, "CoGroupBy");
        }

        public Collection<R3, T> CoGroupBy<K, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<K, IEnumerable<R>, IEnumerable<R2>, IEnumerable<R3>>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            return this.CoGroupBy(other, key1, key2, x => x, x => x, reducer);
        }

        public Collection<R3, T> CoGroupBy<K, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<K, IEnumerable<Weighted<R>>, IEnumerable<Weighted<R2>>, IEnumerable<Weighted<R3>>>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            return this.CoGroupBy(other, key1, key2, x => x, x => x, reducer);
        }

        #endregion GroupBy/CoGroupBy

        #region Join

        public Collection<R3, T> Join<K, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, R2, R3>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");
            var compiledReducer = reducer.Compile();
            
            var that = other as TypedCollection<R2, T>;

            return this.Manufacture<R2,R3>(that, (i, v) => new Operators.Join<K, R, R2, R, R2, T, R3>(i, v, this.Immutable, that.Immutable, key1, key2, x => x, x => x, (k, x, y) => compiledReducer(x, y)), key1.ConvertToWeightedFuncAndHashCode(), key2.ConvertToWeightedFuncAndHashCode(), null, true, "Join");
        }

        public Collection<R3, T> Join<K, V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, V1>> val1, Expression<Func<R2, V2>> val2, Expression<Func<K, V1, V2, R3>> reducer)
            where K : IEquatable<K>
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (val1 == null)
                throw new ArgumentNullException("val1");
            if (val2 == null)
                throw new ArgumentNullException("val2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var compiledReducer = reducer.Compile();

            var that = other as TypedCollection<R2, T>;

            return this.Manufacture<R2, R3>(that, (i, v) => new Operators.Join<K, V1, V2, R, R2, T, R3>(i, v, this.Immutable, that.Immutable, key1, key2, val1, val2, (k, x, y) => compiledReducer(k, x, y)), key1.ConvertToWeightedFuncAndHashCode(), key2.ConvertToWeightedFuncAndHashCode(), null, true, "Join");
        }

        public Collection<R3, T> Join<V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, Int32>> key1, Expression<Func<R2, Int32>> key2, Expression<Func<R, V1>> val1, Expression<Func<R2, V2>> val2, Expression<Func<Int32, V1, V2, R3>> reducer, bool useDenseIntKeys)
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>
        {
            if (other == null)
                throw new ArgumentNullException("other");
            if (key1 == null)
                throw new ArgumentNullException("key1");
            if (key2 == null)
                throw new ArgumentNullException("key2");
            if (val1 == null)
                throw new ArgumentNullException("val1");
            if (val2 == null)
                throw new ArgumentNullException("val2");
            if (reducer == null)
                throw new ArgumentNullException("reducer");
            var otherimpl = other as TypedCollection<R2, T>;
            if (otherimpl == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var compiledReducer = reducer.Compile();
            if (useDenseIntKeys)
            {
                var that = other as TypedCollection<R2, T>;

                return this.Manufacture<R2, R3>(that, (i, v) => new Operators.JoinIntKeyed<V1, V2, R, R2, T, R3>(i, v, this.Immutable, that.Immutable, key1, key2, val1, val2, (k, x, y) => compiledReducer(k, x, y)), key1.ConvertToWeightedFuncAndHashCode(), key2.ConvertToWeightedFuncAndHashCode(), null, true, "Join");
            }
            else
                return this.Join<Int32, V1, V2, R2, R3>(other, key1, key2, val1, val2, reducer);
        }

        #endregion Join

        #region Data-parallel aggregations

        public Collection<R2, T> Aggregate<K, V, R2>(Expression<Func<R, K>> key, Expression<Func<R, V>> value, Expression<Func<Int64, V, V, V>> axpy, Expression<Func<V, bool>> isZero, Expression<Func<K, V, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where V : IEquatable<V>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (axpy == null)
                throw new ArgumentNullException("axpy");
            if (isZero == null)
                throw new ArgumentNullException("isZero");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i, v) => new Operators.Aggregate<K, R, T, R2, V>(i, v, this.Immutable, key, value, axpy, isZero, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Aggregate");
        }

        public Collection<Pair<K, Int64>, T> Count<K>(Expression<Func<R, K>> key)
            where K : IEquatable<K>
        {
            return this.Count(key, (k, c) => new Pair<K, Int64>(k, c))
                       .AssumePartitionedBy(x => x.First);
        }

        public Collection<R2, T> Count<K, R2>(Expression<Func<R, K>> key, Expression<Func<K, Int64, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i, v) => new Operators.Count<K, R, T, R2>(i, v, this.Immutable, key, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Count");
        }

        public Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, int>> valueSelector, Expression<Func<K, int, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (valueSelector == null)
                throw new ArgumentNullException("valueSelector");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i,v) => new Operators.SumInt32<K, R, T, R2>(i, v, this.Immutable, key, valueSelector, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Sum<Int32>");
        }

        public Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, Int64>> valueSelector, Expression<Func<K, Int64, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (valueSelector == null)
                throw new ArgumentNullException("valueSelector");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i, v) => new Operators.SumInt64<K, R, T, R2>(i, v, this.Immutable, key, valueSelector, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Sum<Int64>");
        }

        public Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, float>> valueSelector, Expression<Func<K, float, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (valueSelector == null)
                throw new ArgumentNullException("valueSelector");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i, v) => new Operators.SumFloat<K, R, T, R2>(i, v, this.Immutable, key, valueSelector, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Sum<float>");
        }

        public Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, double>> valueSelector, Expression<Func<K, double, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (valueSelector == null)
                throw new ArgumentNullException("valueSelector");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R2>((i, v) => new Operators.SumDouble<K, R, T, R2>(i, v, this.Immutable, key, valueSelector, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Sum<double>");
        }

        public Collection<R, T> ToStateless()
        {
            return this.Manufacture<R>((i, v) => new Operators.ToStateless<R, T>(i, v, this.Immutable), IdentityWeightedFuncAndHashCode<R>(), IdentityWeightedFuncAndHashCode<R>(), true, "ToStateLess");
        }

        public Collection<R, T> Min<K, M>(Expression<Func<R, K>> key, Expression<Func<R, M>> minBy)
            where K : IEquatable<K>
            where M : IEquatable<M>, IComparable<M>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (minBy == null)
                throw new ArgumentNullException("value");

            var compiledValue = minBy.Compile();

            return this.Manufacture<R>((i, v) => new Operators.Min<K, R, M, R, T>(i, v, this.Immutable, key, x => x, (k, t) => compiledValue(t), (k, t) => t), key.ConvertToWeightedFuncAndHashCode(), key.ConvertToWeightedFuncAndHashCode(), true, "Min");
        }

        public Collection<R, T> Min<V, M>(Expression<Func<R, int>> key, Expression<Func<R, V>> value, Expression<Func<int, V, M>> minBy, Expression<Func<int, V, R>> reducer, bool useDenseIntKeys)
            where V : IEquatable<V>
            where M : IEquatable<M>, IComparable<M>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("selector");
            if (minBy == null)
                throw new ArgumentNullException("value");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            if (useDenseIntKeys)
                return this.Manufacture<R>((i, v) => new Operators.MinIntKeyed<V, M, R, T>(i, v, this.Immutable, key, value, minBy, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Min");
            else
                return this.Min<int, M, V>(key, value, minBy, reducer);
        }

        public Collection<R, T> Min<K, M, V>(Expression<Func<R, K>> key, Expression<Func<R, V>> value, Expression<Func<K,V,M>> minBy, Expression<Func<K, V, R>> reducer)
            where K : IEquatable<K>
            where V : IEquatable<V>
            where M : IEquatable<M>, IComparable<M>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R>((i, v) => new Operators.Min<K, V, M, R, T>(i, v, this.Immutable, key, value, minBy, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Min");
        }

        public Collection<R, T> Max<K, M>(Expression<Func<R, K>> key, Expression<Func<R, M>> value)
            where K : IEquatable<K>
            where M : IComparable<M>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            var compiledValue = value.Compile();

            return this.Manufacture((i, v) => new Operators.Max<K, R, M, R, T>(i, v, this.Immutable, key, x => x, (k, t) => compiledValue(t), (k, t) => t), key.ConvertToWeightedFuncAndHashCode(), key.ConvertToWeightedFuncAndHashCode(), true, "Max");
        }

        public Collection<R, T> Max<K, M, S>(Expression<Func<R, K>> key, Expression<Func<R, S>> selector, Expression<Func<K, S, M>> value, Expression<Func<K, S, R>> reducer)
            where K : IEquatable<K>
            where S : IEquatable<S>
            where M : IComparable<M>
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (selector == null)
                throw new ArgumentNullException("selector");
            if (value == null)
                throw new ArgumentNullException("value");
            if (reducer == null)
                throw new ArgumentNullException("reducer");

            return this.Manufacture<R>((i, v) => new Operators.Max<K, S, M, R, T>(i, v, this.Immutable, key, selector, value, reducer), key.ConvertToWeightedFuncAndHashCode(), null, true, "Max");
        }
        #endregion Data-parallel aggregations

        #region MultiSet operations

        public Collection<R, T> Abs()
        {
            var ident = ((Expression<Func<R, R>>)(x => x)).ConvertToWeightedFuncAndHashCode();

            return this.Manufacture((i, v) => new Operators.Abs<R, T>(i, v, this.Immutable), ident, ident, true, "Abs");
        }

        public Collection<R, T> Distinct()
        {
            var ident = ((Expression<Func<R, R>>)(x => x)).ConvertToWeightedFuncAndHashCode();

            return this.Manufacture((i, v) => new Operators.Distinct<R, T>(i, v, this.Immutable), ident, ident, true, "Distinct");
        }

        public Collection<R, T> Union(Collection<R, T> other)
        {
            if (other == null)
                throw new ArgumentNullException("other");
            var that = other as TypedCollection<R, T>;
            if (that == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var ident = ((Expression<Func<R,R>>) (x => x)).ConvertToWeightedFuncAndHashCode();

            return this.Manufacture(that, (i, v) => new Operators.Union<R, T>(i, v, this.Immutable, that.Immutable), ident, ident, ident, true, "Union");
        }

        public Collection<R, T> Intersect(Collection<R, T> other)
        {
            if (other == null)
                throw new ArgumentNullException("other");
            var that = other as TypedCollection<R, T>;
            if (that == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var ident = ((Expression<Func<R, R>>)(x => x)).ConvertToWeightedFuncAndHashCode();

            return this.Manufacture(that, (i, v) => new Operators.Intersect<R, T>(i, v, this.Immutable, that.Immutable), ident, ident, ident, true, "Intersect");
        }

        public Collection<R, T> SymmetricDifference(Collection<R, T> other)
        {
            if (other == null)
                throw new ArgumentNullException("other");
            var that = other as TypedCollection<R, T>;
            if (that == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R2, T>", "other");

            var ident = ((Expression<Func<R, R>>)(x => x)).ConvertToWeightedFuncAndHashCode();

            return this.Manufacture(that, (i, v) => new Operators.SymmetricDifference<R, T>(i, v, this.Immutable, that.Immutable), ident, ident, ident, true, "SymmetricDifference");

        }

        public Collection<R, T> Concat(Collection<R, T> other)
        {
            if (other == null)
                throw new ArgumentNullException("other");
            var that = other as TypedCollection<R, T>;
            if (that == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R, T>", "other");

            var partitionFunction = this.Output.PartitionedBy;
            if (!Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(this.Output.PartitionedBy, that.Output.PartitionedBy))
                partitionFunction = null;

            return this.Manufacture(that, (i, v) => new Operators.Concat<R, T>(i, v), partitionFunction, partitionFunction, partitionFunction, false, "Concat");
        }

        public Collection<R, T> Except(Collection<R, T> other)
        {
            if (other == null)
                throw new ArgumentNullException("other");
            var that = other as TypedCollection<R, T>;
            if (that == null)
                throw new ArgumentException("Other collection must implement TypedCollection<R, T>", "other");

            var partitionFunction = this.Output.PartitionedBy;
            if (!Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(this.Output.PartitionedBy, that.Output.PartitionedBy))
                partitionFunction = null;

            return this.Manufacture(that, (i, v) => new Operators.Except<R, T>(i, v), partitionFunction, partitionFunction, partitionFunction, false, "Except");
        }

        #endregion MultiSet operations

        #region Fixed Point

        /// <summary>
        /// Adds a temporal dimension to each record.
        /// </summary>
        /// <param name="context">Loop context</param>
        /// <returns></returns>
        public Collection<R, IterationIn<T>> EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T> context)
        {
            return context.EnterLoop(this.Output).ToCollection(this.Immutable);
        }

        /// <summary>
        /// Adds a temporal dimension to each record.
        /// </summary>
        /// <param name="context">Loop context</param>
        /// <param name="initialIteration">initial iteration selector</param>
        /// <returns></returns>
        public Collection<R, IterationIn<T>> EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T> context, Func<R, int> initialIteration)
        {
            return context.EnterLoop(this.Output, (Weighted<R> x) => initialIteration(x.record)).ToCollection(this.Immutable);
        }

        public Collection<R, T> GeneralFixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>, Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, // (lc, x) => f(x)
            Func<R, int> priorityFunction,
            Expression<Func<R, K>> partitionedBy,
            int maxIterations, CheckpointType delayCheckpoint)
        {
            if (priorityFunction == null)
                throw new ArgumentNullException("priorityFunction");

            var compiled = partitionedBy.ConvertToWeightedFuncAndHashCode();

            var fp = new Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>(this.Context);

            // probably doesn't work correctly when max + pri >= 2^31. Fix!
            var delayVertex = fp.Delay(compiled, maxIterations);
            delayVertex.Output.SetCheckpointType(delayCheckpoint);

            // consider partitioning first, to ensure even boring work is distributed
            var ingress = fp.EnterLoop(this.Output, x => priorityFunction(x.record))
                            .ToCollection()
                            .PartitionBy(partitionedBy);

            var source = ingress.Concat(delayVertex.Output.ToCollection());

            var iteration = f(fp, source);

            var fixedpoint = iteration.Except(ingress)
                                      .Consolidate(partitionedBy);

            delayVertex.Input = fixedpoint.Output;

            var egress = fp.ExitLoop(iteration.Output).ToCollection();

            return egress;
        }

        public Collection<R, T> FixedPoint(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f)
        {
            return this.FixedPoint(f, Int32.MaxValue);
        }
        public Collection<R, T> FixedPoint(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, int maxIterations)
        {
            return this.FixedPoint<int>(f, null, maxIterations);
        }
        public Collection<R, T> FixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, Expression<Func<R, K>> consolidateFunction)
        {
            return this.FixedPoint(f, consolidateFunction, Int32.MaxValue);
        }
        public Collection<R, T> FixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, Expression<Func<R, K>> consolidateFunction,
            int maxIterations)
        {
            return this.FixedPoint(f, consolidateFunction, maxIterations, CheckpointType.Stateless);
        }
        public Collection<R, T> FixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, Expression<Func<R, K>> consolidateFunction,
            int maxIterations, CheckpointType delayCheckpoint)
        {
            if (f == null)
                throw new ArgumentNullException("f");

            var compiled = consolidateFunction.ConvertToWeightedFuncAndHashCode();

            var fp = new Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>(this.Context);

            var delay = fp.Delay<Weighted<R>>(compiled, maxIterations);
            delay.Output.SetCheckpointType(delayCheckpoint);

            var ingress = fp.EnterLoop<Weighted<R>>(this.Output).ToCollection()
                .PartitionBy(consolidateFunction);                              // add coordinate and ensure partitioned appropriately.

            var source = ingress.Concat(delay.Output.ToCollection());          // merge input with feedback data.

            var iteration = f(fp, source);                                      // apply the body of the logic.

            var fixedpoint = iteration.Except(ingress)                          // subtract starting point.
                                      .Consolidate(consolidateFunction);        // consolidate, to ensure cancellation.

            delay.Input = fixedpoint.Output;                                 // attach the result to the delayVertex source.

            return fp.ExitLoop(iteration.Output).ToCollection();                  // return the loop body as output.
        }

        #endregion Fixed Point

        #region Monitoring

        /// <summary>
        /// Monitors records passing through.
        /// </summary>
        /// <param name="action">Action to be applied to each group of records</param>
        /// <returns>Input collection</returns>
        public Collection<R, T> Monitor(Action<int, List<Pair<Weighted<R>, T>>> action)
        {
            return this.Manufacture((i,v) => new Operators.Monitor<R, T>(i, v, this.Immutable, action), this.Output.PartitionedBy, this.Output.PartitionedBy, false, "Monitor");
        }

        #endregion Monitoring

        #endregion Naiad Operators

        internal abstract StreamContext Context { get; }

        #region Constructor
        internal TypedCollection() { }

        // internal TypedCollection(TypedCollection<R, T> decoratee) : base(decoratee) { }
        #endregion Constructor
    }

    internal class DataflowCollection<R, T> : TypedCollection<R, T>
        where R : IEquatable<R>
        where T : Time<T>
    {
        private readonly Stream<Weighted<R>, T> output;

        public override Stream<Weighted<R>, T> Output
        {
            get { return this.output; }
        }

        internal override StreamContext Context
        {
            get { return this.output.Context; }
        }

        public DataflowCollection(Stream<Weighted<R>, T> output)
        {
            this.output = output;
        }
    }

    internal class InternalCollection<R, T> : TypedCollection<R, T>
        where R : IEquatable<R>
        where T : Time<T>
    {
        private readonly Stream<Weighted<R>,T> output;

        public override Stream<Weighted<R>, T> Output { get { return this.output; } }

        internal override StreamContext Context
        {
            get { return output.Context; }
        }

        public override string ToString()
        {
            return output.ForStage.ToString();
        }

        public InternalCollection(Stream<Weighted<R>, T> output)
            : this(output, false)
        {
        }
        public InternalCollection(Stream<Weighted<R>, T> output, bool immutable)
        {
            this.output = output;
            this.immutable = immutable;
        }
    }
}
