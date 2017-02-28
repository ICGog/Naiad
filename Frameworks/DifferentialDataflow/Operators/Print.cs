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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Print<S, T> : UnaryVertex<Weighted<S>, Weighted<S>, T>
        where S : IEquatable<S>
        where T : Time<T>
    {

        Dictionary<T, int> NumRecs;
        string Name;
        System.Diagnostics.Stopwatch stopwatch;

        public override void OnReceive(Message<Weighted<S>, T> message)
        {
            this.NotifyAt(message.time);
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
              int num;
              if (this.NumRecs.TryGetValue(message.time, out num))
              {
                NumRecs[message.time] = num + 1;
              }
              else
              {
                NumRecs[message.time] = 1;
              }
              output.Send(message.payload[i].record.ToWeighted(message.payload[i].weight));
            }
        }

        public override void OnNotify(T workTime)
        {
          long microseconds = this.stopwatch.ElapsedTicks * 1000000L / System.Diagnostics.Stopwatch.Frequency;
          Console.WriteLine(String.Format("{0:D11}: {1} total events at time {2} is {3}",
                                          microseconds, Name, workTime, NumRecs[workTime]));
          base.OnNotify(workTime);
        }

        public Print(int index, Stage<T> collection, string name,
                     System.Diagnostics.Stopwatch stopwatch) : base(index, collection)
        {
            this.NumRecs = new Dictionary<T, int>();
            this.Name = name;
            this.stopwatch = stopwatch;
        }
    }
}
