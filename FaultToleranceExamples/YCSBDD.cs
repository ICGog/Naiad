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
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
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

  public static class ExtensionMethods
  {

    public static Stream<string, T> AdVertex<T>(this Stream<long, T> stream,
                                                BatchedDataSource<string> input,
                                                string[] preparedAds,
                                                long numEventsPerEpoch,
                                                long timeSliceLengthMs)
      where T : Time<T>
    {
      return stream.NewUnaryStage<long, string, T>((i, s) => new YCSBDD.AdVertex<T>(i, s, input, preparedAds, numEventsPerEpoch, timeSliceLengthMs), null, null, "AdVertex");
    }

    public static Stream<Pair<string, long>, T> RedisVertex<T>(this Stream<YCSBDD.AdEventProjected, T> stream,
                                                               Dictionary<string, string> adCampaign)
      where T : Time<T>
    {
      return stream.NewUnaryStage<YCSBDD.AdEventProjected, Pair<string, long>, T>((i, s) => new YCSBDD.RedisVertex<T>(i, s, adCampaign), null, null, "RedisVertex");
    }

    public static Stream<long, T> RedisCampaignVertex<T>(this Stream<Pair<Pair<string, long>, long>, T> stream,
                                                         ConnectionMultiplexer redis)
      where T : Time<T>
    {
      return stream.NewUnaryStage<Pair<Pair<string, long>, long>, long, T>((i, s) => new YCSBDD.RedisCampaignVertex<T>(i, s, redis), null, null, "RedisCampaignVertex");
    }
  }

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

    public class RedisCampaignVertex<T> : UnaryVertex<Pair<Pair<string, long>, long>, long, T>
      where T: Time<T>
    {
      private IDatabase redisDB;
      private DateTime dt1970;
      private Dictionary<T, Dictionary<string, long>> cache;
      private Dictionary<T, long> epochToTime;
      private long windowDuration = 10000;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<Pair<Pair<string, long>, long>, T> message)
      {
        Console.WriteLine("RedisCampaignVertex " + message.time);
        this.NotifyAt(message.time);
        for (int i = 0; i < message.length; i++) {
          string campaignId = message.payload[i].First.First;
          long campaignTime = message.payload[i].First.Second;
          epochToTime[message.time] = campaignTime;
          long campaignCount = message.payload[i].Second;
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
        }
      }

      public override void OnNotify(T time)
      {
        long campaignTime;
        if (epochToTime.TryGetValue(time, out campaignTime)) {
          TimeSpan span = DateTime.UtcNow - dt1970;
          long latency = Convert.ToInt64(span.TotalMilliseconds) - campaignTime - windowDuration;
          foreach (KeyValuePair<string, long> entry in cache[time]) {
            writeWindow(entry.Key, campaignTime, entry.Value);
          }
          cache.Remove(time);
          epochToTime.Remove(time);
        }
        base.OnNotify(time);
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

      public RedisCampaignVertex(int index, Stage<T> stage, ConnectionMultiplexer redis) : base(index, stage)
      {
        redisDB = redis.GetDatabase();
        dt1970 = new DateTime(1970, 1, 1);
        cache = new Dictionary<T, Dictionary<string, long>>();
        epochToTime = new Dictionary<T, long>();
      }
    }

    public class AdVertex<T> : UnaryVertex<long, string, T>
      where T: Time<T>
    {
      private string[] preparedAds;
      private int adsIdx;
      private long numEventsPerEpoch;
      private BatchedDataSource<string> input;
      private string[] adEvents;
      private long timeSliceLengthMs;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<long, T> message)
      {
        Console.WriteLine("AdVertex received " + message.time);
        string tailAd = message.payload[0] + "\",\"ip_address\":\"1.2.3.4\"}";
        long emitStartTime = getCurrentTime();
        for (int i = 0; i < this.numEventsPerEpoch; i++) {
          if (this.adsIdx == this.preparedAds.Length) {
            this.adsIdx = 0;
          }
          adEvents[i] = preparedAds[this.adsIdx++] + tailAd;
        }
        input.OnNext(adEvents);
        long emitEndTime = getCurrentTime();
        if (emitEndTime - message.payload[0] > timeSliceLengthMs) {
          long behind = emitEndTime - message.payload[0] - timeSliceLengthMs;
          Console.WriteLine("Falling behind by " + behind + "ms while emitting " + numEventsPerEpoch + " events");
        }
      }

      public AdVertex(int index, Stage<T> stage,
                      BatchedDataSource<string> input,
                      string[] preparedAds, long numEventsPerEpoch, long timeSliceLengthMs) : base(index, stage)
      {
        this.input = input;
        this.preparedAds = preparedAds;
        this.adsIdx = 0;
        this.numEventsPerEpoch = numEventsPerEpoch;
        this.adEvents = new string[this.numEventsPerEpoch];
        this.timeSliceLengthMs = timeSliceLengthMs;
      }
    }

    public class RedisVertex<T> : UnaryVertex<AdEventProjected, Pair<string, long>, T>
      where T: Time<T>
    {
      private Dictionary<string, string> adCampaign;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<AdEventProjected, T> message)
      {
        var output = this.Output.GetBufferForTime(message.time);
        for (int i = 0; i < message.length; i++)
        {
          string campaignId = adCampaign[message.payload[i].AdId];
          long campaignTime = 10000 * (Convert.ToInt64(message.payload[i].EventTime) / 10000);
          output.Send(campaignId.PairWith(campaignTime));
        }
      }

      public RedisVertex(int index, Stage<T> stage,
                         Dictionary<string, string> adCampaign) : base(index, stage)
      {
        this.adCampaign = adCampaign;
      }
    }

    Configuration config;

    public void Execute(string[] args)
    {
      this.config = Configuration.FromArgs(ref args);
//      this.config.MaxLatticeInternStaleTimes = 50;
      this.config.DefaultCheckpointInterval = 1000;

      string ycsbConfigFile = "";
      string logPrefix = "/tmp/falkirk/";
      bool minimalLogging = false;
      int managerWorkerCount = 1;
      bool nonIncrementalFTManager = false;
      long loadTargetHz = 100000;
      long timeSliceLengthMs = 1000;
      long numElementsToGenerate = 60L * loadTargetHz;
      string replayEventsPath = "";
      bool singleThreadGenerator = false;
      bool lindiGenerator = false;
      bool enableFT = false;
      bool enableFailure = false;
      long failAfterMs = 11000;
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
        case "-replayevents":
          replayEventsPath = args[i + 1];
          i += 2;
          break;
        case "-singlethreadgenerator":
          singleThreadGenerator = true;
          i++;
          break;
        case "-lindigenerator":
          lindiGenerator = true;
          i++;
          break;
        case "-ft":
          enableFT = true;
          i++;
          break;
        case "-enablefailure":
          enableFailure = true;
          i++;
          break;
        case "-failafter":
          failAfterMs = Int64.Parse(args[i + 1]);
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

      FTManager manager = null;
      if (enableFT) {
        System.IO.Directory.CreateDirectory(logPrefix);
        this.config.LogStreamFactory = (s => new FileLogStream(logPrefix, s));
        System.IO.Directory.CreateDirectory(Path.Combine(logPrefix, "checkpoint"));
        this.config.CheckpointingFactory = s => new FileStreamSequence(Path.Combine(logPrefix, "checkpoint"), s);

        manager = new FTManager(this.config.LogStreamFactory, null, null,
                                !nonIncrementalFTManager);
      }

      ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisHost);

//      Placement inputPlacement = new Placement.ProcessRange(Enumerable.Range(0, 2).Concat(Enumerable.Range(0, 1)), Enumerable.Range(0, 4));

      using (var computation = NewComputation.FromConfig(this.config))
      {
        long numEventsPerEpoch = loadTargetHz * timeSliceLengthMs / 1000 / computation.Configuration.Processes;

        YCSBEventGenerator eventGenerator =
          new YCSBEventGenerator(loadTargetHz, timeSliceLengthMs, numElementsToGenerate);
        var campaigns = eventGenerator.getCampaigns();

        if (computation.Configuration.ProcessID == 0) {
          eventGenerator.prepareRedis(redis);
        }

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

        var batchedAdInput = new BatchedDataSource<string>();
        var batchedAdInputStream = computation.NewInput(batchedAdInput).SetCheckpointType(CheckpointType.CachingInput).SetCheckpointPolicy(s => new CheckpointEagerly());
        var adCollectionInput = computation.NewInputCollection<string>();
        var campaignTimeInput = computation.NewInputCollection<Pair<Pair<string, long>, long>>();
        //.SetCheckpointType(CheckpointType.CachingInput).SetCheckpointPolicy(s => new CheckpointEagerly());
        var windowInput = new BatchedDataSource<Pair<int, long>>();
        var windowInputStream = computation.NewInput(windowInput).SetCheckpointType(CheckpointType.StatelessLogEphemeral);//.PartitionBy(x => (int)(Convert.ToInt64(x) % 5));

        if (!singleThreadGenerator) {
          if (lindiGenerator) {
            windowInputStream.PartitionBy(x => x.First).Select(x => x.Second)
              .AdVertex(batchedAdInput, eventGenerator.getPreparedAds(), numEventsPerEpoch / computation.Configuration.WorkerCount, timeSliceLengthMs).SetCheckpointType(CheckpointType.StatelessLogEphemeral);
            var campaignsTime = batchedAdInputStream
              .Select(jsonString => Newtonsoft.Json.JsonConvert.DeserializeObject<AdEvent>(jsonString)).SetCheckpointType(CheckpointType.StatelessLogEphemeral)
              .Where(adEvent => adEvent.event_type.Equals("view")).SetCheckpointType(CheckpointType.StatelessLogEphemeral)
              .Select(adEvent => new AdEventProjected(adEvent.ad_id, adEvent.event_time)).SetCheckpointType(CheckpointType.StatelessLogEphemeral)
              .RedisVertex(adsToCampaign).SetCheckpointType(CheckpointType.StatelessLogEphemeral)
              .Count().SetCheckpointType(CheckpointType.StatelessLogEphemeral);
//              .RedisCampaignVertex(redis).SetCheckpointType(CheckpointType.StatelessLogEphemeral);
            campaignsTime.Subscribe((ii, l) => campaignTimeInput.OnNext(l));

            var result = campaignTimeInput.RedisCampaignProcessor<string>(x => x.First.First, x => x.First.Second, x => x.Second, redis).SetCheckpointType(CheckpointType.StatelessLogEphemeral);

            if (computation.Configuration.ProcessID == 0 && enableFT) {
              manager.Initialize(computation,
//                                 new int[] { campaignsTime.ForStage.StageId, result.Output.ForStage.StageId },
                                 new int[] { campaignsTime.ForStage.StageId },
                                 managerWorkerCount, minimalLogging);
            }
          } else {
            var campaignsTime = adCollectionInput
              .AdEventGenerator((preparedAd, tailAd) => preparedAd + tailAd,
                                eventGenerator.getPreparedAds(), numEventsPerEpoch, timeSliceLengthMs,
                                x => Convert.ToInt64(x))
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

            if (computation.Configuration.ProcessID == 0 && enableFT) {
              manager.Initialize(computation,
                                 new int[] { result.Output.ForStage.StageId },
                                 managerWorkerCount, minimalLogging);
            }
          }
        }

        computation.Activate();

        // if (computation.Configuration.ProcessID == 0)
        // {
        //   YCSB.KafkaConsumer kafkaConsumer = new YCSB.KafkaConsumer(kafkaBrokers, kafkaTopic);
        //   kafkaConsumer.StartConsumer(adCollectionInput, computation);
        // }

        if (!singleThreadGenerator) {
          long numIter = numElementsToGenerate / loadTargetHz * 1000 / timeSliceLengthMs;
          long beginWindow = getCurrentTime();
          long startTime = beginWindow;
          Thread.Sleep((int)((beginWindow / 10000) * 10000 + 20000 - beginWindow));
          beginWindow = (beginWindow / 10000) * 10000 + 20000;
          for (int epoch = 0; epoch < numIter; ++epoch) {
            if (lindiGenerator) {
              List<Pair<int, long>> threadWindowInput = new List<Pair<int, long>>();
              int startIndex = computation.Configuration.ProcessID * computation.Configuration.WorkerCount;
              for (int threadIndex = startIndex; threadIndex < startIndex + computation.Configuration.WorkerCount; ++threadIndex) {
                threadWindowInput.Add(threadIndex.PairWith(beginWindow));
              }
              windowInput.OnNext(threadWindowInput);
            } else {
              adCollectionInput.OnNext(beginWindow.ToString());
            }

            if (computation.Configuration.ProcessID == 0 && enableFailure) {
              long sinceStart = getCurrentTime() - startTime;
              if (sinceStart <= failAfterMs && beginWindow + timeSliceLengthMs - startTime >= failAfterMs) {
                long sleepToFailureTime = failAfterMs - sinceStart;
                if (sleepToFailureTime > 0) {
                  Thread.Sleep((int)sleepToFailureTime);
                }
                IEnumerable<int> pauseImmediately = Enumerable.Range(0, computation.Configuration.Processes);
                List<int> pauseAfterRecovery = new List<int>();
                List<int> pauseLast = new List<int>();
                HashSet<int> failedProcesses = new HashSet<int>();
                failedProcesses.Add(1);
                manager.FailProcess(failedProcesses, 0, 1);
                manager.PerformRollback(pauseImmediately,
                                        pauseAfterRecovery,
                                        pauseLast);
              }
            }

            long sleepTime = timeSliceLengthMs + beginWindow - getCurrentTime();
            if (sleepTime > 0) {
              Thread.Sleep((int)sleepTime);
            } else {
              Console.WriteLine("Falling behind by " + (-sleepTime) + " with epoch generation");
            }
            beginWindow += timeSliceLengthMs;
          }
        } else if (!replayEventsPath.Equals("")) {
          StreamReader file = new StreamReader(replayEventsPath);
          string line;
          int lineIndex = 0;
          List<string> evs = new List<string>();
          while((line = file.ReadLine()) != null) {
            lineIndex++;
            if (lineIndex % 10000 == 0) {
              adCollectionInput.OnNext(evs);
              evs.Clear();
            }
            evs.Add(line);
          }
        }

        windowInput.OnCompleted();
        adCollectionInput.OnCompleted();
        campaignTimeInput.OnCompleted();
        batchedAdInput.OnCompleted();

        computation.Join();
        if (computation.Configuration.ProcessID == 0 && enableFT)
        {
          manager.Join();
        }
      }
    }

    public static DateTime dt1970 = new DateTime(1970, 1, 1);

    public static long getCurrentTime()
    {
      TimeSpan span = DateTime.UtcNow - dt1970;
      return Convert.ToInt64(span.TotalMilliseconds);
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
