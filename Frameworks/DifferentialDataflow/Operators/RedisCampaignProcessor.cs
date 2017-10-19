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

using StackExchange.Redis;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{

  internal class RedisCampaignProcessorVertex<S, T, R> : UnaryVertex<Weighted<S>, Weighted<R>, T>
    where S: IEquatable<S>
    where T: Time<T>
    where R: IEquatable<R>
  {
    private IDatabase redisDB;
    private DateTime dt1970;
    private Dictionary<T, Dictionary<string, long>> cache;
    private Dictionary<T, long> epochToTime;
    public Func<S, string> campaignSelector;
    public Func<S, long> timeSelector;
    public Func<S, long> countSelector;
    private long windowDuration = 10000;

    public override void OnReceive(Message<Weighted<S>, T> message) {
      this.NotifyAt(message.time);
      var output = this.Output.GetBufferForTime(message.time);
      for (int i = 0; i < message.length; i++)
      {
        string campaignId = this.campaignSelector(message.payload[i].record);
        long campaignTime = this.timeSelector(message.payload[i].record);
        epochToTime[message.time] = campaignTime;
        long campaignCount = this.countSelector(message.payload[i].record);
        Dictionary<string, long> campaignCache;
        if (cache.TryGetValue(message.time, out campaignCache)) {
          long value;
          if (campaignCache.TryGetValue(campaignId, out value)) {
            campaignCache[campaignId] = value + campaignCount;
          } else {
            campaignCache[campaignId] = campaignCount;
          }
        } else {
          cache[message.time] = new Dictionary<string, long>();
          cache[message.time][campaignId] = campaignCount;
        }
        // long timeDiff = writeWindow(campaignId, campaignTime, campaignCount);
        // TimeSpan span = DateTime.UtcNow - dt1970;
        // long curTime = Convert.ToInt64(span.TotalMilliseconds);
        // long timeDiff = curTime - campaignTime;
        // Console.WriteLine(timeDiff);
      }
    }

    public override void OnNotify(T time) {
      // Flush campaign.
      long campaignTime;
      if (epochToTime.TryGetValue(time, out campaignTime))
      {
        TimeSpan span = DateTime.UtcNow - dt1970;
        long latency = Convert.ToInt64(span.TotalMilliseconds) - campaignTime - windowDuration;
//        Console.WriteLine("Finished window " + campaignTime + " at " + curTime);
        if (latency > 0)
          Console.WriteLine("Latency: " + latency);
        foreach (KeyValuePair<string, long> entry in cache[time])
        {
          writeWindow(entry.Key, campaignTime, entry.Value);
        }
        cache.Remove(time);
        epochToTime.Remove(time);
      }
      base.OnNotify(time);
    }

    public RedisCampaignProcessorVertex(int index, Stage<T> stage,
                                        Expression<Func<S, string>> campaignFunc,
                                        Expression<Func<S, long>> timeFunc,
                                        Expression<Func<S, long>> countFunc,
                                        ConnectionMultiplexer redis)
      : base(index, stage) {
      redisDB = redis.GetDatabase();
      dt1970 = new DateTime(1970, 1, 1);
      cache = new Dictionary<T, Dictionary<string, long>>();
      epochToTime = new Dictionary<T, long>();
      campaignSelector = campaignFunc.Compile();
      timeSelector = timeFunc.Compile();
      countSelector = countFunc.Compile();
    }

    private long writeWindow(String campaignId, long campaignTime, long campaignCount)
    {
      String cTimeStr = campaignTime.ToString();
      String windowUUID = redisDB.HashGet(campaignId, cTimeStr);
      if (windowUUID == null) {
        windowUUID = System.Guid.NewGuid().ToString();
        redisDB.HashSet(campaignId, cTimeStr, windowUUID);
        String windowListUUID = redisDB.HashGet(campaignId, "windows");
        if (windowListUUID == null) {
          windowListUUID = System.Guid.NewGuid().ToString();
          redisDB.HashSet(campaignId, "windows", windowListUUID);
        }
        redisDB.ListLeftPush(windowListUUID, cTimeStr);
      }
      TimeSpan span = DateTime.UtcNow - dt1970;
      long curTime = Convert.ToInt64(span.TotalMilliseconds);
      redisDB.HashIncrement(windowUUID, "seen_count", campaignCount);
      redisDB.HashSet(windowUUID, "time_updated", curTime.ToString());
      redisDB.ListLeftPush("time_updated", curTime.ToString());
      return curTime - campaignTime;
    }
  }
}
