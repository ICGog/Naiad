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
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;


using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators;

using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Diagnostics;

namespace FaultToleranceExamples.YCSBDD
{
  public class YCSBEventGenerator
  {
    private int adsIdx = 0;
    private int eventsIdx = 0;
    private StringBuilder sb = new StringBuilder();
    private string pageID = System.Guid.NewGuid().ToString();
    private string userID = System.Guid.NewGuid().ToString();
    private string[] eventTypes = { "view", "click", "purchase" };
    private string eventHeader;
    private List<string> ads;
    private Dictionary<string, List<string>> campaigns;

    private long loadTargetHz;
    private long timeSliceLengthMs;
    private long totalEventsToGenerate;
    private DateTime dt1970;
    private long beginTs;
    private long elementsGenerated = 0;

    public YCSBEventGenerator(long loadTargetHz, long timeSliceLengthMs, long totalEventsToGenerate)
    {
      this.campaigns = generateCampaigns();
      this.ads = flattenCampaigns();

      this.loadTargetHz = loadTargetHz;
      this.timeSliceLengthMs = timeSliceLengthMs;
      this.totalEventsToGenerate = totalEventsToGenerate;
      this.dt1970 = new DateTime(1970, 1, 1);
      this.beginTs = getCurrentTime();
      this.eventHeader = "{\"user_id\":\"" + pageID + "\",\"page_id\":\"" + userID + "\",\"ad_id\":\"";
    }

    public Dictionary<string, List<string>> getCampaigns() {
      return campaigns;
    }

    public long getCurrentTime()
    {
      TimeSpan span = DateTime.UtcNow - dt1970;
      return Convert.ToInt64(span.TotalMilliseconds);
    }

    private long loadPerTimeslice(long numTasks)
    {
      long messagesPerOperator = this.loadTargetHz / numTasks;
      return messagesPerOperator * this.timeSliceLengthMs / 1000;
    }

    public void run(long numTasks, InputCollection<string> kafkaInput)
    {
      long elements = loadPerTimeslice(numTasks);
      long totalElementsPerTask = totalEventsToGenerate / numTasks;
      if (elementsGenerated == 0) {
        beginTs = getCurrentTime();
      }
      string[] input = new string[elements];
      string[] preparedAds = new string[ads.Count];
      for (int i = 0; i < ads.Count; i++)
      {
        preparedAds[i] = this.eventHeader + ads[i] +
          "\",\"ad_type\":\"banner78\",\"event_type\":\"" +
          eventTypes[i % eventTypes.Length];
      }
      while (elementsGenerated < totalElementsPerTask) {
        long emitStartTime = getCurrentTime();
        long sliceTs = beginTs + (this.timeSliceLengthMs * (elementsGenerated / elements));
        string eventTail = "\",\"event_time\":\"" + sliceTs + "\",\"ip_address\":\"1.2.3.4\"}";
        for (int i = 0; i < elements; i++) {
          if (adsIdx == ads.Count) {
            adsIdx = 0;
          }
          input[i] = preparedAds[adsIdx++] + eventTail;
//          input.Add(generateElement(sliceTs));
        }
        long addingTime = getCurrentTime();
        Console.WriteLine("Adding " + input.Length);
        Console.WriteLine("Adding took " + (addingTime - emitStartTime).ToString());
        kafkaInput.OnNext(input);
        long onNextTime = getCurrentTime() - addingTime;
        Console.WriteLine("OnNext took " + onNextTime);
        elementsGenerated += elements;
        long emitEndTime = getCurrentTime();
        if (emitEndTime < (sliceTs + this.timeSliceLengthMs)) {
          Thread.Sleep(Convert.ToInt32(sliceTs + this.timeSliceLengthMs - emitEndTime));
        } else {
          Console.WriteLine("FALLING BEHIND by " + (emitEndTime - sliceTs - this.timeSliceLengthMs));
        }
      }
    }

    public string generateElement(long sliceTs)
    {
      if (adsIdx == ads.Count) {
        adsIdx = 0;
      }
      if (eventsIdx == eventTypes.Length) {
        eventsIdx = 0;
      }
      sb.Clear();
      sb.Append(this.eventHeader);
      sb.Append(ads[adsIdx++]);
      sb.Append("\",\"ad_type\":\"banner78\",\"event_type\":\"");
      sb.Append(eventTypes[eventsIdx++]);
      sb.Append("\",\"event_time\":\"");
      sb.Append(sliceTs);
      sb.Append("\",\"ip_address\":\"1.2.3.4\"}");
      return sb.ToString();
    }

    public Dictionary<string, List<string>> generateCampaigns()
    {
      int numCampaigns = 100;
      int numAdsPerCampaign = 10;
      Dictionary<string, List<string>> adsByCampaign = new Dictionary<string, List<string>>();
      for (int i = 0; i < numCampaigns; i++) {
        string campaign = System.Guid.NewGuid().ToString();
        List<string> ads = new List<string>();
        for (int j = 0; j < numAdsPerCampaign; j++) {
          ads.Add(System.Guid.NewGuid().ToString());
        }
        adsByCampaign[campaign] = ads;
      }
      return adsByCampaign;
    }

    public List<string> flattenCampaigns()
    {
      List<string> ads = new List<string>();
      foreach (KeyValuePair<string, List<string>> entry in campaigns) {
        foreach (string ad in entry.Value)
        {
          ads.Add(ad);
        }
      }
      var rnd = new Random(42);
      return ads.OrderBy(item => rnd.Next()).ToList();
    }
  }

}