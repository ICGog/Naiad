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

using Confluent.Kafka;

using StackExchange.Redis;

using YamlDotNet.RepresentationModel;

namespace FaultToleranceExamples.YCSBDD
{

  public class YCSBDD : Example
  {

    private class FileLogStream : LogStream
    {
      private StreamWriter log;
      private FileStream logFile;
      public StreamWriter Log
      {
        get { return log; }
      }

      public void Flush()
      {
        lock (log)
        {
          log.Flush();
          logFile.Flush(true);
        }
      }

      private void FlushFileThread()
      {
        while (true)
        {
          Thread.Sleep(1000);
          this.Flush();
        }
      }

      public FileLogStream(string prefix, string fileName)
      {
        this.logFile = new FileStream(Path.Combine(prefix, fileName), FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        this.log = new StreamWriter(this.logFile);
        var flush = new System.Threading.Thread(
                      new System.Threading.ThreadStart(() => this.FlushFileThread()));
        flush.Start();
      }
    }

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

    public interface IAdEvent
    {
      string AdId { get; set; }
      string EventTime { get; set; }
    }

    public struct AdEventProjected : IAdEvent, IEquatable<AdEventProjected>
    {
      public string adId;
      public string AdId { get { return this.adId; } set { this.adId = value; } }
      public string eventTime;
      public string EventTime { get { return this.eventTime; } set { this.eventTime = value; } }

      public AdEventProjected(string adId, string eventTime)
      {
        this.adId = adId;
        this.eventTime = eventTime;
      }

      public bool Equals(AdEventProjected other)
      {
        return adId.Equals(other.adId) &&
          eventTime.Equals(other.eventTime);
      }

      public override int GetHashCode()
      {
        return adId.GetHashCode() + 1234347 * eventTime.GetHashCode();
      }

      public override string ToString()
      {
        return adId + " " + eventTime;
      }
    }

    Configuration config;

    public void Execute(string[] args)
    {
      this.config = Configuration.FromArgs(ref args);
      this.config.MaxLatticeInternStaleTimes = 50;
//      this.config.DefaultCheckpointInterval = 1000;

      string ycsbConfigFile = "";
      string logPrefix = "/tmp/falkirk/";
      bool minimalLogging = false;
      int managerWorkerCount = 4;
      bool nonIncrementalFTManager = false;
      long loadTargetHz = 100000;
      long timeSliceLengthMs = 1000;
      long numElementsToGenerate = 60L * loadTargetHz;
      int i = 1;
      while (i < args.Length)
      {
        switch (args[i].ToLower())
        {
        case "-minimallog":
          minimalLogging = true;
          ++i;
          break;
        case "-nonincrementalftmanager":
          nonIncrementalFTManager = true;
          i++;
          break;
        case "-mwc":
          managerWorkerCount = Int32.Parse(args[i + 1]);
          i += 2;
          break;
        case "-logprefix":
          logPrefix = args[i + 1];
          i += 2;
          break;
        case "-ycsbconfigfile":
          ycsbConfigFile = args[i + 1];
          i += 2;
          break;
        case "-loadtarget":
          loadTargetHz = Int64.Parse(args[i + 1]);
          i += 2;
          break;
        case "-timeslice":
          timeSliceLengthMs = Int64.Parse(args[i + 1]);
          i += 2;
          break;
        case "-numelements":
          numElementsToGenerate = Int64.Parse(args[i + 1]);
          i += 2;
          break;
        default:
          throw new ApplicationException("Unknown argument " + args[i]);
        }
      }

      var conf = findAndReadConfigFile(ycsbConfigFile, true);

      string kafkaBrokers = getKafkaBrokers(conf);
      string kafkaTopic = conf["kafka.topic"].ToString();
      int kafkaPartitions = Int32.Parse(conf["kafka.partitions"].ToString());
      string zkServers = getZookeeperServers(conf);
      string redisHost = conf["redis.host"].ToString();

      System.IO.Directory.CreateDirectory(logPrefix);
//      this.config.LogStreamFactory = (s => new FileLogStream(logPrefix, s));

      System.IO.Directory.CreateDirectory(Path.Combine(logPrefix, "checkpoint"));
//      this.config.CheckpointingFactory = s => new FileStreamSequence(Path.Combine(logPrefix, "checkpoint"), s);

      // FTManager manager = new FTManager(this.config.LogStreamFactory,
      //                                   null, null,
      //                                   !nonIncrementalFTManager);

      ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisHost);

      using (var computation = NewComputation.FromConfig(this.config))
      {
        var kafkaInput = computation.NewInputCollection<string>();

        YCSBEventGenerator eventGenerator =
          new YCSBEventGenerator(loadTargetHz, timeSliceLengthMs, numElementsToGenerate);
        var campaigns = eventGenerator.getCampaigns();
        Dictionary<string, string> adsToCampaign = new Dictionary<string, string>();
        foreach (KeyValuePair<string, List<string>> entry in campaigns) {
          foreach (string ad in entry.Value) {
            adsToCampaign[ad] = entry.Key;
          }
        }

        // var result = kafkaInput
        //   .RedisCampaign2(x => x, (campaignId, eventTime) => campaignId.PairWith(10000 * (Convert.ToInt64(eventTime) / 10000)), adsToCampaign)
        //   .Count(x => x)
        //   .RedisCampaignProcessor<string>(x => x.First.First, x => x.First.Second, x => x.Second, redis);

        var campaignsTime = kafkaInput
          .Select(jsonString => Newtonsoft.Json.JsonConvert.DeserializeObject<AdEvent>(jsonString))
          .Where(adEvent => adEvent.event_type.Equals("view"))
          .Select(adEvent => new AdEventProjected(adEvent.ad_id, adEvent.event_time))
          .RedisCampaign(adEvent => adEvent.AdId, adEvent => adEvent.EventTime,
                         (campaignId, eventTime) => campaignId.PairWith(eventTime),
                         redis, adsToCampaign);

        var result = campaignsTime
          .Select(campaignTime => campaignTime.First.PairWith(10000 * (Convert.ToInt64(campaignTime.Second) / 10000)))
          .Count(x => x)
          .RedisCampaignProcessor<string>(x => x.First.First, x => x.First.Second, x => x.Second, redis);

        // if (computation.Configuration.ProcessID == 0)
        // {
        //   manager.Initialize(computation,
        //                      new int[] { projected.ForStage.StageId },
        //                      managerWorkerCount, minimalLogging);
        // }

//        var output = result.Subscribe(l => { });

        computation.Activate();

        // if (computation.Configuration.ProcessID == 0)
        // {
        //   YCSB.KafkaConsumer kafkaConsumer = new YCSB.KafkaConsumer(kafkaBrokers, kafkaTopic);
        //   kafkaConsumer.StartConsumer(kafkaInput, computation);
        // }

        eventGenerator.run(computation.Configuration.Processes, kafkaInput);

        // StreamReader file = new StreamReader("/home/srguser/falkirk/Naiad/ad_events.in");
        // string line;
        // int z = 0;
        // List<string> evs = new List<string>();
        // while((line = file.ReadLine()) != null) {
        //   z++;
        //   if (z % 10000 == 0)
        //   {
        //     kafkaInput.OnNext(evs);
        //     evs.Clear();
        //   }
        //   evs.Add(line);
        // }

        kafkaInput.OnCompleted();

        computation.Join();

        // if (computation.Configuration.ProcessID == 0)
        // {
        //   manager.Join();
        // }
      }
    }

    private static string getKafkaBrokers(Dictionary<string, Object> conf) {
      if (!conf.ContainsKey("kafka.brokers")) {
        throw new Exception("No Kafka brokers found");
      }
      if (!conf.ContainsKey("kafka.port")) {
        throw new Exception("No Kafka port found");
      }
      string port = conf["kafka.port"].ToString();
      string brokers = conf["kafka.brokers"].ToString().Replace("[", "").Replace("]", "").Replace(" ", "").Replace(",", ":" + port + ",");
      brokers += ":" + port;
      return brokers;
    }

    private static string getZookeeperServers(Dictionary<string, Object> conf) {
      if (!conf.ContainsKey("zookeeper.servers")) {
        throw new Exception("No Zookeeper servers found");
      }
      if (!conf.ContainsKey("zookeeper.port")) {
        throw new Exception("No Zookeeper port found");
      }
      string port = conf["zookeeper.port"].ToString();
      string servers = conf["zookeeper.servers"].ToString().Replace("[", "").Replace("]", "").Replace(" ", "").Replace(",", ":" + port + ",");
      servers += ":" + port;
      return servers;
    }

    private static Dictionary<string, Object> findAndReadConfigFile(string name, bool mustExist)
    {
      StreamReader input = null;
      bool configFileEmpty = false;
      try {
        input = new StreamReader(name);
        if (input != null) {
          var yaml = new YamlStream();
          yaml.Load(input);
          var mapping =
            (YamlMappingNode)yaml.Documents[0].RootNode;
          var config = new Dictionary<string, Object>();
          foreach (var entry in mapping.Children) {
            config[((YamlScalarNode)entry.Key).Value] = entry.Value;
          }
          return config;
        }
        if (mustExist) {
          if (configFileEmpty) {
            throw new Exception("Config file " + name + " doesn't have any valid configs");
          } else {
            throw new Exception("Could not find config file " + name);
          }
        } else {
          return new Dictionary<string, Object>();
        }
      } finally {
        if (input != null)
        {
          input.Close();
        }
      }
    }

    public string Usage { get { return ""; } }

    public string Help
    {
      get { return ""; }
    }
  }
}
