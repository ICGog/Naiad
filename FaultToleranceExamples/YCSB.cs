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
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Diagnostics;

using Confluent.Kafka;

using StackExchange.Redis;

using YamlDotNet.RepresentationModel;

namespace FaultToleranceExamples.YCSB
{

  public static class ExtensionMethods
  {

    public static Stream<Pair<string, string>, T> RedisQuery<R, T>(this Stream<R, T> input, ConnectionMultiplexer redis)
      where R : YCSB.IAdEvent
      where T : Time<T>
    {
      return Foundry.NewUnaryStage(input, (i, s) => new RedisQueryVertex<R, T>(i, s, redis),
                                   x => x.GetHashCode(), x => x.GetHashCode(), "RedisQuery");
    }

    public class RedisQueryVertex<R, T> : UnaryVertex<R, Pair<string, string>, T>
      where R : YCSB.IAdEvent
      where T : Time<T>
    {
      private IDatabase redisDB;
      private Dictionary<string, string> adCampaign;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<R, T> message) {
        var output = this.Output.GetBufferForTime(message.time);
        for (int i = 0; i < message.length; i++)
        {
          string campaignId = execute(message.payload[i].AdId);
          if (campaignId != null)
          {
            string eventTime = message.payload[i].EventTime;
            // Output: (campaign_id, event_time)
            output.Send(new Pair<string, string>(campaignId, eventTime));
          }
        }
      }

      public string execute(string adId)
      {
        if (adCampaign.ContainsKey(adId)) {
          return adCampaign[adId];
        } else {
          string campaign_id = redisDB.StringGet(adId);
          if (campaign_id != null) {
            adCampaign[adId] = campaign_id;
          }
          return campaign_id;
        }
      }

      public override void OnNotify(T time) {
      }

      public RedisQueryVertex(int index, Stage<T> stage, ConnectionMultiplexer redis) : base(index, stage) {
        adCampaign = new Dictionary<string, string>();
        redisDB = redis.GetDatabase();
      }
    }

    public static Stream<string, T> CampaignProcessor<T>(this Stream<Pair<Pair<string, long>, long>, T> input, ConnectionMultiplexer redis)
      where T: Time<T>
    {
      return Foundry.NewUnaryStage(input, (i, s) => new CampaignProcessorVertex<T>(i, s, redis),
                                   x => x.GetHashCode(), x => x.GetHashCode(), "CampaignProcessor");
    }

    public class CampaignProcessorVertex<T> : UnaryVertex<Pair<Pair<string, long>, long>, string, T>
      where T: Time<T>
    {
      private IDatabase redisDB;
      private DateTime dt1970;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<Pair<Pair<string, long>, long>, T> message) {
        var output = this.Output.GetBufferForTime(message.time);
        for (int i = 0; i < message.length; i++)
        {
          string campaignId = message.payload[i].First.First;
          long campaignTime = message.payload[i].First.Second;
          long campaignCount = message.payload[i].Second;
          long timeDiff = writeWindow(campaignId, campaignTime, campaignCount);
          output.Send(timeDiff.ToString());
        }
      }

      public override void OnNotify(T time) {
      }

      public CampaignProcessorVertex(int index, Stage<T> stage, ConnectionMultiplexer redis) : base(index, stage) {
        redisDB = redis.GetDatabase();
        dt1970 = new DateTime(1970, 1, 1);
      }

      private long writeWindow(string campaignId, long campaignTime, long campaignCount)
      {
        string cTimeStr = campaignTime.ToString();
        string windowUUID = redisDB.HashGet(campaignId, campaignTime);
        if (windowUUID == null) {
          windowUUID = System.Guid.NewGuid().ToString();
          redisDB.HashSet(campaignId, cTimeStr, windowUUID);
          string windowListUUID = redisDB.HashGet(campaignId, "windows");
          if (windowListUUID == null) {
            windowListUUID = System.Guid.NewGuid().ToString();
            redisDB.HashSet(campaignId, "windows", windowListUUID);
          }
          redisDB.ListLeftPush(windowListUUID, cTimeStr);
        }
        TimeSpan span = DateTime.UtcNow - dt1970;
        long curTime = Convert.ToInt64(span.TotalMilliseconds);
        redisDB.HashIncrement(windowUUID, "seen_count", campaignCount);
        redisDB.HashSet(windowUUID, "time_updated", curTime);
        return curTime - campaignTime;
      }
    }

  }

  public class YCSB : Example
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

    public struct Window : IEquatable<Window>
    {
      public string timestamp;
      public long seenCount;

      public bool Equals(Window other)
      {
        return timestamp.Equals(other.timestamp);
      }

      public override int GetHashCode()
      {
        return 31 + timestamp.GetHashCode();
      }

      public override string ToString()
      {
        return "{ time: " + timestamp + ", seen: " + seenCount + " }";
      }
    }

    public struct CampaignWindowPair : IEquatable<CampaignWindowPair>
    {
      public string campaign;
      public Window window;

      public CampaignWindowPair(string campaign, Window window)
      {
        this.campaign = campaign;
        this.window = window;
      }

      public bool Equals(CampaignWindowPair other)
      {
        return campaign.Equals(other.campaign) &&
          window.Equals(other.window);
      }

      public override int GetHashCode()
      {
        int result = 31 + campaign.GetHashCode();
        result = result * 31 + window.GetHashCode();
        return result;
      }

      public override string ToString()
      {
        return "{ " + campaign + " : " + window.ToString() + " }";
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
      this.config.MaxLatticeInternStaleTimes = 10;
      this.config.DefaultCheckpointInterval = 1000;

      string ycsbConfigFile = "";
      string logPrefix = "/tmp/falkirk/";
      bool minimalLogging = false;
      int managerWorkerCount = 4;
      bool nonIncrementalFTManager = false;
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
        var kafkaInput = new BatchedDataSource<string>();
        var kafkaInputStream = computation.NewInput(kafkaInput);

        Stream<Pair<string, string>, Epoch> campaignsTime = kafkaInputStream
          .Select(jsonString => Newtonsoft.Json.JsonConvert.DeserializeObject<AdEvent>(jsonString))
          .Where(adEvent => adEvent.event_type.Equals("view"))
          .Select(adEvent => new AdEventProjected(adEvent.ad_id, adEvent.event_time))
          .RedisQuery<AdEventProjected, Epoch>(redis);

        var result = campaignsTime
          .Select(campaignTime => campaignTime.First.PairWith(10000 * (Convert.ToInt64(campaignTime.Second) / 10000)))
          .Count()
          .CampaignProcessor(redis);

        var output = result.Subscribe(l => { foreach (var x in l) Console.WriteLine(x); });

        // if (computation.Configuration.ProcessID == 0)
        // {
        //   manager.Initialize(computation,
        //                      new int[] { projected.ForStage.StageId },
        //                      managerWorkerCount, minimalLogging);
        // }

        computation.Activate();
        if (computation.Configuration.ProcessID == 0)
        {
          KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaBrokers, kafkaTopic);
//          var thread = new System.Threading.Thread(new System.Threading.ThreadStart(() => kafkaConsumer.StartConsumer(kafkaInput, computation)));
//          thread.Start();
          kafkaConsumer.StartConsumer(kafkaInput, computation);
        }

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