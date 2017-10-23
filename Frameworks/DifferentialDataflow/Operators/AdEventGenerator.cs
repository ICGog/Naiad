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
  internal class AdEventGeneratorVertex<S, T, R> : UnaryVertex<Weighted<S>, Weighted<R>, T>
    where S: IEquatable<S>
    where T: Time<T>
    where R: IEquatable<R>
  {
    private Func<string, string, R> resultCreator;
    private string[] preparedAds;
    private int adsIdx;
    private long numEventsPerEpoch;
    private DateTime dt1970;

    public override void OnReceive(Message<Weighted<S>, T> message)
    {
      var output = this.Output.GetBufferForTime(message.time);
      for (int j = 0; j < message.length; j++)
      {
        long emitStartTime = getCurrentTime();
        for (int i = 0; i < this.numEventsPerEpoch; i++) {
          if (this.adsIdx == this.preparedAds.Length) {
            this.adsIdx = 0;
          }
          string eventTail = getCurrentTime() + "\",\"ip_address\":\"1.2.3.4\"}";
          output.Send((this.resultCreator(preparedAds[this.adsIdx++], eventTail)).ToWeighted(1));
        }
        long emitEndTime = getCurrentTime();
        Console.WriteLine("Generating " + this.numEventsPerEpoch + " records took " + (emitEndTime - emitStartTime));
      }
    }

    private long getCurrentTime()
    {
      TimeSpan span = DateTime.UtcNow - dt1970;
      return Convert.ToInt64(span.TotalMilliseconds);
    }

    public AdEventGeneratorVertex(int index, Stage<T> collection,
                                  Expression<Func<string, string, R>> resultFunc,
                                  string[] preparedAds, long numEventsPerEpoch)
      : base(index, collection) {
      this.resultCreator = resultFunc.Compile();
      this.preparedAds = preparedAds;
      this.adsIdx = 0;
      this.numEventsPerEpoch = numEventsPerEpoch;
      this.dt1970 = new DateTime(1970, 1, 1);
    }
  }
}