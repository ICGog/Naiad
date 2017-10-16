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
  internal class RedisCampaignVertex<S, T, R> : UnaryVertex<Weighted<S>, Weighted<R>, T>
    where S: IEquatable<S>
    where T: Time<T>
    where R: IEquatable<R>
  {
    public Func<S, string> adSelector;
    public Func<S, string> timeSelector;
    public Func<string, string, R> resultCreator;
    private IDatabase redisDB;
    private Dictionary<string, string> adCampaign;

    public override void OnReceive(Message<Weighted<S>, T> message) {
      var output = this.Output.GetBufferForTime(message.time);
      for (int i = 0; i < message.length; i++)
      {
        string campaignId = execute(this.adSelector(message.payload[i].record));
        if (campaignId != null)
        {
          string eventTime = this.timeSelector(message.payload[i].record);
          // Output: (campaign_id, event_time)
          // TODO(ionel): Check if the weight logic is correct.
          output.Send(this.resultCreator(campaignId, eventTime).ToWeighted(message.payload[i].weight));
        }
      }
    }

    public string execute(string adId)
    {
      if (adCampaign.ContainsKey(adId)) {
        return adCampaign[adId];
      } else {
        string campaign_id = redisDB.StringGet(adId);
//        string campaign_id = adId;
        if (campaign_id != null) {
          adCampaign[adId] = campaign_id;
        }
        return campaign_id;
      }
    }

    public RedisCampaignVertex(int index, Stage<T> collection,
                               Expression<Func<S, string>> adFunc,
                               Expression<Func<S, string>> timeFunc,
                               Expression<Func<string, string, R>> resultFunc,
                               ConnectionMultiplexer redis)
      : base(index, collection) {
      adCampaign = new Dictionary<string, string>();
      redisDB = redis.GetDatabase();
      adSelector = adFunc.Compile();
      timeSelector = timeFunc.Compile();
      resultCreator = resultFunc.Compile();
    }
  }
}