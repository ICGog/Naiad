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
    public struct AdEvent : IEquatable<AdEvent>
    {
      public string user_id;
      public string page_id;
      public string ad_id;
      public string ad_type;
      public string event_type;
      public string event_time;
      public string ip_address;

      public AdEvent(string user_id, string page_id, string ad_id, string ad_type,
                     string event_type, string event_time, string ip_address) {
        this.user_id = user_id;
        this.page_id = page_id;
        this.ad_id = ad_id;
        this.ad_type = ad_type;
        this.event_type = event_type;
        this.event_time = event_time;
        this.ip_address = ip_address;
      }

      public bool Equals(AdEvent other)
      {
        return user_id.Equals(other.user_id) &&
          page_id.Equals(other.page_id) &&
          ad_id.Equals(other.ad_id) &&
          ad_type.Equals(other.ad_type) &&
          event_type.Equals(other.event_type) &&
          event_time.Equals(other.event_time) &&
          ip_address.Equals(other.ip_address);
      }

      public override int GetHashCode()
      {
        return ad_id.GetHashCode() + 1234347 * event_type.GetHashCode() +
          4311 * event_time.GetHashCode() + 31 * user_id.GetHashCode() +
          17 * page_id.GetHashCode() + ad_type.GetHashCode() * 7 +
          ip_address.GetHashCode();;
      }

      public override string ToString()
      {
        return user_id + " " + page_id + " " + ad_id + " " + ad_type + " " +
          event_type + " " + event_time + " " + ip_address;
      }

    }

  internal class RedisCampaignVertex2<S, T, R> : UnaryVertex<Weighted<S>, Weighted<R>, T>
    where S: IEquatable<S>
    where T: Time<T>
    where R: IEquatable<R>
  {
    public Func<S, string> adSelector;
    public Func<string, string, R> resultCreator;
    private Dictionary<string, string> adCampaign;

    public override void OnReceive(Message<Weighted<S>, T> message) {
      var output = this.Output.GetBufferForTime(message.time);
      for (int i = 0; i < message.length; i++)
      {
        string adString = this.adSelector(message.payload[i].record);
        AdEvent adEvent = Newtonsoft.Json.JsonConvert.DeserializeObject<AdEvent>(adString);
        if (adEvent.event_type.Equals("view"))
        {
          output.Send(this.resultCreator(adCampaign[adEvent.ad_id], adEvent.event_time).ToWeighted(message.payload[i].weight));
        }
      }
    }

    public RedisCampaignVertex2(int index, Stage<T> collection,
                                Expression<Func<S, string>> adFunc,
                                Expression<Func<string, string, R>> resultFunc,
                                Dictionary<string, string> adsToCampaign)
      : base(index, collection) {
      adCampaign = adsToCampaign;
      adSelector = adFunc.Compile();
      resultCreator = resultFunc.Compile();
    }
  }
}