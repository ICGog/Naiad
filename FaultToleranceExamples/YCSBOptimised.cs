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

namespace FaultToleranceExamples.YCSBOptimised
{

  public static class ExtensionMethods
  {

    public static Stream<Pair<string, long>, T> AdVertex<T>(this Stream<long, T> stream,
                                                            BatchedDataSource<Pair<string, long>> input,
                                                            string[] preparedAds,
                                                            long numEventsPerEpoch,
                                                            long timeSliceLengthMs,
                                                            Dictionary<string, string> adCampaign)
      where T : Time<T>
    {
      return stream.NewUnaryStage<long, Pair<string, long>, T>((i, s) => new YCSBOptimised.AdVertex<T>(i, s, input, preparedAds, numEventsPerEpoch, timeSliceLengthMs, adCampaign), null, null, "AdVertex");
    }

    public static Stream<long, T> RedisCampaignVertex<T>(this Stream<Pair<Pair<string, long>, long>, T> stream,
                                                         ConnectionMultiplexer redis)
      where T : Time<T>
    {
      return stream.NewUnaryStage<Pair<Pair<string, long>, long>, long, T>((i, s) => new YCSBOptimised.RedisCampaignVertex<T>(i, s, redis), null, null, "RedisCampaignVertex");
    }
  }

  public class YCSBOptimised : Example
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
//        Console.WriteLine("RedisCampaignVertex " + message.time);
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

    public class AdVertex<T> : UnaryVertex<long, Pair<string, long>, T>
      where T: Time<T>
    {
      private Dictionary<string, string> adCampaign;
      private string[] preparedAds;
      private int adsIdx;
      private long numEventsPerEpoch;
      private BatchedDataSource<Pair<string, long>> input;
      private long timeSliceLengthMs;
      private int index;
      private List<Pair<string, long>> adEvents;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<long, T> message)
      {
        Console.WriteLine("AdVertex received " + message.time + " bla " + index);
        adEvents.Clear();
        string tailAd = message.payload[0] + "\",\"ip_address\":\"1.2.3.4\"}";
        long emitStartTime = getCurrentTime();
        Char[] splitter = new Char[] { '"' };
        for (int i = 0; i < this.numEventsPerEpoch; i++) {
          if (this.adsIdx == this.preparedAds.Length) {
            this.adsIdx = 0;
          }
          //adEvents[i] = preparedAds[this.adsIdx++] + getCurrentTime() + "\",\"ip_address\":\"1.2.3.4\"}";
          var adEvent = (preparedAds[this.adsIdx++] + tailAd).Split(splitter);
          if (adEvent[19].Equals("view")) {
            string campaignId = adCampaign[adEvent[11]];
            long campaignTime = 10000 * (Convert.ToInt64(adEvent[23]) / 10000);
            adEvents.Add(campaignId.PairWith(campaignTime));
          }
        }
        input.OnNext(adEvents);
        long emitEndTime = getCurrentTime();
        if (emitEndTime - message.payload[0] > timeSliceLengthMs) {
          long behind = emitEndTime - message.payload[0] - timeSliceLengthMs;
          Console.WriteLine("Falling behind by " + behind + "ms while emitting " + numEventsPerEpoch + " events");
        }
      }

      public AdVertex(int index, Stage<T> stage,
                      BatchedDataSource<Pair<string, long>> input,
                      string[] preparedAds, long numEventsPerEpoch, long timeSliceLengthMs,
                      Dictionary<string, string> adCampaign) : base(index, stage)
      {
        this.index = index;
        this.adCampaign = adCampaign;
        this.input = input;
        this.preparedAds = preparedAds;
        this.adsIdx = 0;
        this.numEventsPerEpoch = numEventsPerEpoch;
        this.timeSliceLengthMs = timeSliceLengthMs;
        this.adEvents = new List<Pair<string, long>>();
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
        long numEventsPerEpoch = loadTargetHz * timeSliceLengthMs / 1000 / computation.Configuration.Processes / computation.Configuration.WorkerCount;

        YCSBDD.YCSBEventGenerator eventGenerator =
          new YCSBDD.YCSBEventGenerator(loadTargetHz, timeSliceLengthMs, numElementsToGenerate);
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

        var batchedAdInput = new BatchedDataSource<Pair<string, long>>();
        var batchedAdInputStream = computation.NewInput(batchedAdInput).SetCheckpointType(CheckpointType.CachingInput);//.SetCheckpointPolicy(s => new CheckpointEagerly());
        var windowInput = new BatchedDataSource<Pair<int, long>>();
        var windowInputStream = computation.NewInput(windowInput).SetCheckpointType(CheckpointType.StatelessLogEphemeral);//.PartitionBy(x => (int)(Convert.ToInt64(x) % 5));

        windowInputStream.PartitionBy(x => x.First).Select(x => x.Second)
          .AdVertex(batchedAdInput, eventGenerator.getPreparedAds(),
                    numEventsPerEpoch, timeSliceLengthMs, adsToCampaign).SetCheckpointType(CheckpointType.StatelessLogEphemeral);
        var campaignsTime = batchedAdInputStream
          .Count().SetCheckpointType(CheckpointType.StatelessLogEphemeral)
          .RedisCampaignVertex(redis).SetCheckpointType(CheckpointType.StatelessLogEphemeral);

        if (computation.Configuration.ProcessID == 0 && enableFT) {
          manager.Initialize(computation,
                             new int[] { campaignsTime.ForStage.StageId },
                             managerWorkerCount, minimalLogging);
        }

        computation.Activate();

        long numIter = numElementsToGenerate / loadTargetHz * 1000 / timeSliceLengthMs;
        long beginWindow = getCurrentTime();
        long startTime = beginWindow;
        Thread.Sleep((int)((beginWindow / 10000) * 10000 + 10000 - beginWindow));
        beginWindow = (beginWindow / 10000) * 10000 + 10000;
        for (int epoch = 0; epoch < numIter; ++epoch) {
          List<Pair<int, long>> threadWindowInput = new List<Pair<int, long>>();
          for (int threadIndex = 0; threadIndex < computation.Configuration.WorkerCount; ++threadIndex) {
              threadWindowInput.Add(threadIndex.PairWith(beginWindow));
          }
          windowInput.OnNext(threadWindowInput);
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

        windowInput.OnCompleted();
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
