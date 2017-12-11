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

namespace FaultToleranceExamples.YCSBSelective
{

  public static class ExtensionMethods
  {

    public static Stream<YCSBSelective.AdEvent, T> AdGen<T>(this Stream<long, T> stream,
                                              string[] adsIds,
                                              long numEventsPerEpoch,
                                              long timeSliceLengthMs)
      where T : Time<T>
    {
      return stream.NewUnaryStage<long, YCSBSelective.AdEvent, T>((i, s) => new YCSBSelective.AdGenVertex<T>(i, s, adsIds, numEventsPerEpoch, timeSliceLengthMs), null, null, "AdGenVertex");
    }

    public static Stream<Pair<string, long>, T> AdProc<T>(this Stream<YCSBSelective.AdEvent, T> stream,
                                                          Dictionary<string, string> adCampaign)
      where T : Time<T>
    {
      return stream.NewUnaryStage<YCSBSelective.AdEvent, Pair<string, long>, T>((i, s) => new YCSBSelective.AdProcVertex<T>(i, s, adCampaign), null, null, "AdProcVertex");
    }

    public static Stream<long, T> RedisCampaignVertex<T>(this Stream<Pair<Pair<string, long>, long>, T> stream,
                                                         ConnectionMultiplexer redis)
      where T : Time<T>
    {
      return stream.NewUnaryStage<Pair<Pair<string, long>, long>, long, T>((i, s) => new YCSBSelective.RedisCampaignVertex<T>(i, s, redis), null, null, "RedisCampaignVertex");
    }
  }

  public class YCSBSelective : Example
  {

    public static long windowDuration = 1000;

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
      private Dictionary<Pointstamp, Dictionary<string, long>> cache;
      private Dictionary<Pointstamp, long> epochToTime;
      private int index;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<Pair<Pair<string, long>, long>, T> message)
      {
        Console.WriteLine(getCurrentTime() + " RedisCampaignVertex " + index + " received " + message.time);
        this.NotifyAt(message.time);
        Pointstamp epoch = message.time.ToPointstamp(index);
        for (int i = 0; i < message.length; i++) {
          string campaignId = message.payload[i].First.First;
          long campaignTime = message.payload[i].First.Second;
          epochToTime[epoch] = campaignTime;
          long campaignCount = message.payload[i].Second;
//          writeWindow(campaignId, campaignTime, campaignCount);
          Dictionary<string, long> campaignCache;
          if (cache.TryGetValue(epoch, out campaignCache)) {
            long value;
            if (campaignCache.TryGetValue(campaignId, out value)) {
              campaignCache[campaignId] = value + campaignCount;
            } else {
              campaignCache[campaignId] = campaignCount;
            }
          } else {
            cache[epoch] = new Dictionary<string, long>();
            cache[epoch][campaignId] = campaignCount;
          }
        }
      }

      public override void OnNotify(T time)
      {
        Console.WriteLine(getCurrentTime() + " RedisCampaignVertex " + index + " notify " + time);
        if (!exactlyOnce) {
          Pointstamp epoch = time.ToPointstamp(index);
          long campaignTime;
          if (epochToTime.TryGetValue(epoch, out campaignTime)) {
            foreach (KeyValuePair<string, long> entry in cache[epoch]) {
              writeWindow(entry.Key, campaignTime, entry.Value);
            }
            cache.Remove(epoch);
            epochToTime.Remove(epoch);
          }
          if (epoch.Timestamp[1] > maxEpoch)
            waitStable.Signal();
        }
        Console.WriteLine(getCurrentTime() + " RedisCampaignVertex " + index + " end notify " + time);
        base.OnNotify(time);
      }

      protected override bool CanRollBackPreservingState(Pointstamp[] frontier)
      {
        return true;
      }

      protected override bool MustRollBackPreservingState(Pointstamp[] frontier)
      {
        return true;
      }

      public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<T> lastFullCheckpoint, ICheckpoint<T> lastIncrementalCheckpoint)
      {
        base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);
        if (frontier.Length == 0) {
          cache.Clear();
          epochToTime.Clear();
        } else {
          Pointstamp lastEpoch = frontier[0];
          var toRemove = this.epochToTime.Keys.Where(epoch => epoch.CompareTo(lastEpoch) > 0).ToArray();
          foreach (var epoch in toRemove) {
            this.cache.Remove(epoch);
            this.epochToTime.Remove(epoch);
          }
        }
      }

      public override void NotifyGarbageCollectionFrontier(Pointstamp[] frontier)
      {
        if (exactlyOnce) {
          var epochsToRelease = frontier[0];
          Console.WriteLine(getCurrentTime() + " RedisCampaignVertex " + index + " notify GC " + epochsToRelease);
          var timesToRelease = this.epochToTime.Where(time => time.Key.CompareTo(epochsToRelease) <= 0).OrderBy(time => time.Key).ToArray();
          foreach (var time in timesToRelease) {
            long campaignTime;
            if (epochToTime.TryGetValue(time.Key, out campaignTime)) {
              foreach (KeyValuePair<string, long> entry in cache[time.Key]) {
                writeWindow(entry.Key, campaignTime, entry.Value);
              }
              cache.Remove(time.Key);
              epochToTime.Remove(time.Key);
              waitStable.Signal();
            }
          }
        }
      }

      private long writeWindow(String campaignId, long campaignTime, long campaignCount)
      {
        String cTimeStr = campaignTime.ToString();
        String windowUUID = redisDB.HashGet(campaignId, cTimeStr);
        if (windowUUID == null) {
          // TODO(ionel): Should use a campaignId/cTimeStr seeded GUID generator, but
          // there isn't one available in C#. This causes Redis counts to not be
          // aggregated correctly. However, all ads are countet and flow through the
          // system.
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
        return curTime - campaignTime;
      }

      public RedisCampaignVertex(int index, Stage<T> stage, ConnectionMultiplexer redis) : base(index, stage)
      {
        redisDB = redis.GetDatabase();
        dt1970 = new DateTime(1970, 1, 1);
        cache = new Dictionary<Pointstamp, Dictionary<string, long>>();
        epochToTime = new Dictionary<Pointstamp, long>();
        this.index = index;
      }
    }

    public class AdGenVertex<T> : UnaryVertex<long, YCSBSelective.AdEvent, T>
      where T: Time<T>
    {
      private string pageID;
      private string userID;
      private string[] eventTypes = { "view", "click", "purchase" };
      private string[] adsIds;
      private int adsIdx;
      private long numEventsPerEpoch;
      private long timeSliceLengthMs;
      private int index;
      private int called;

      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<long, T> message)
      {
        Console.WriteLine(getCurrentTime() + " AdGenVertex " + index + " received " + message.time);

        long emitStartTime = getCurrentTime();
        var output = this.Output.GetBufferForTime(message.time);
        for (int index = 0; index < message.length; index++) {
          long toGenerate = this.numEventsPerEpoch / 2;
          if (called == 2)
          {
            toGenerate = 1;
          }
          for (int i = 0; i < toGenerate; i++) {
            if (this.adsIdx == this.adsIds.Length) {
              this.adsIdx = 0;
            }
            output.Send(new AdEvent(userID,
                                    pageID,
                                    adsIds[this.adsIdx++],
                                    "banner78",
                                    eventTypes[adsIdx % eventTypes.Length],
                                    message.payload[index],
                                    "1.2.3.4"));
          }
          called++;
        }
        Console.WriteLine(getCurrentTime() + " AdGenVertex " + index + " received end " + message.time);
      }

      public AdGenVertex(int index, Stage<T> stage,
                         string[] adsIds, long numEventsPerEpoch,
                         long timeSliceLengthMs) : base(index, stage)
      {
        pageID = System.Guid.NewGuid().ToString();
        userID = System.Guid.NewGuid().ToString();
        this.index = index;
        this.called = 0;
        this.adsIds = adsIds;
        this.adsIdx = 0;
        this.numEventsPerEpoch = numEventsPerEpoch;
        this.timeSliceLengthMs = timeSliceLengthMs;
      }
    }

    public class AdProcVertex<T> : UnaryVertex<AdEvent, Pair<string, long>, T>
      where T: Time<T>
    {
      private Dictionary<string, string> adCampaign;
      private int index;
      public override void OnReceive(Microsoft.Research.Naiad.Dataflow.Message<AdEvent, T> message)
      {
//        Console.WriteLine(getCurrentTime() + " AdProcVertex " + index + " received " + message.time);
        var output = this.Output.GetBufferForTime(message.time);
        for (int i = 0; i < message.length; i++) {
          if (message.payload[i].event_type.Equals("view"))
          {
            string campaignId = adCampaign[message.payload[i].ad_id];
            long campaignTime = windowDuration * (message.payload[i].event_time / windowDuration);
            output.Send(campaignId.PairWith(campaignTime));
          }
        }
  //      Console.WriteLine(getCurrentTime() + " AdProcVertex " + index + " received end " + message.time);
      }

      public AdProcVertex(int index, Stage<T> stage,
                          Dictionary<string, string> adCampaign) : base(index, stage)
      {
        this.index = index;
        this.adCampaign = adCampaign;
      }
    }

    public struct AdEvent : IEquatable<AdEvent>
    {
      public string user_id;
      public string page_id;
      public string ad_id;
      public string ad_type;
      public string event_type;
      public long event_time;
      public string ip_address;

      public AdEvent(string user_id, string page_id, string ad_id, string ad_type,
                     string event_type, long event_time, string ip_address) {
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
          event_time == other.event_time &&
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
      bool nonSelective = false;
      long delayEachEpoch = -1;
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
        case "-nonselective":
          nonSelective = true;
          i++;
          break;
        case "-delayeachepoch":
          delayEachEpoch = Int64.Parse(args[i + 1]);
          i += 2;
          break;
        case "-exactlyonce":
          exactlyOnce = true;
          i++;
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

        var windowInput = new SubBatchDataSource<Pair<int, long>, Epoch>();
        var windowInputStream = computation.NewInput(windowInput).SetCheckpointType(CheckpointType.CachingInput).SetCheckpointPolicy(s => new CheckpointEagerly());

        var campaignsTime = windowInputStream.PartitionBy(x => x.First).SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(c => new CheckpointEagerly())
          .Select(x => x.Second).SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(c => new CheckpointEagerly())
          .AdGen(eventGenerator.getAdIds(), numEventsPerEpoch, timeSliceLengthMs).SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(c => new CheckpointEagerly())
          .AdProc(adsToCampaign).SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(c => new CheckpointEagerly())
          .Count(c => new CheckpointEagerly()).SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(c => new CheckpointEagerly())
          .RedisCampaignVertex(redis).SetCheckpointType(CheckpointType.Stateless).SetCheckpointPolicy(c => new CheckpointEagerly());

        if (computation.Configuration.ProcessID == 0 && enableFT) {
          manager.Initialize(computation,
                             new int[] { campaignsTime.ForStage.StageId },
                             managerWorkerCount, minimalLogging);
        }

        computation.Activate();

        waitStable = new CountdownEvent(computation.Configuration.WorkerCount);

        long numIter = numElementsToGenerate / loadTargetHz * 1000 / timeSliceLengthMs;
        long beginWindow = getCurrentTime();
        long startTime = beginWindow;
        Thread.Sleep((int)((beginWindow / windowDuration) * windowDuration + windowDuration - beginWindow));
        beginWindow = (beginWindow / windowDuration) * windowDuration + windowDuration;
        bool failedProc = false;
        Epoch outerBatch = new Epoch(0);
        int currentSubBatch = 0;
        int delayAtEpoch = 30;
        for (int epoch = 0; epoch < numIter; ++epoch) {
          List<Pair<int, long>> threadWindowInput = new List<Pair<int, long>>();
          int startIndex = computation.Configuration.ProcessID * computation.Configuration.WorkerCount;
          for (int threadIndex = startIndex; threadIndex < startIndex + computation.Configuration.WorkerCount; ++threadIndex) {
              threadWindowInput.Add(threadIndex.PairWith(beginWindow));
          }
          BatchIn<Epoch> currentTime = new BatchIn<Epoch>(outerBatch, currentSubBatch);
          if (!nonSelective ||
              (delayEachEpoch == -1 && epoch != delayAtEpoch) ||
              (delayEachEpoch != -1 && (epoch % delayEachEpoch != (delayEachEpoch - 1) || epoch < 20))) {
            windowInput.OnDataAtTime(threadWindowInput, currentTime);
          }
          long sleepTime = 400 + beginWindow - getCurrentTime();
          if (sleepTime > 0) {
            Thread.Sleep((int)sleepTime);
          } else {
            Console.WriteLine("Falling behind by " + (-sleepTime) + " with first input");
          }

          if ((delayEachEpoch == -1 && epoch == delayAtEpoch) ||
              (delayEachEpoch != -1 && epoch % delayEachEpoch == (delayEachEpoch - 1) && epoch >= 20)) {
            List<Pair<int, long>> prevThreadWindowInput = new List<Pair<int, long>>();
            startIndex = computation.Configuration.ProcessID * computation.Configuration.WorkerCount;
            for (int threadIndex = startIndex; threadIndex < startIndex + computation.Configuration.WorkerCount; ++threadIndex) {
              prevThreadWindowInput.Add(threadIndex.PairWith(beginWindow - windowDuration));
            }
            BatchIn<Epoch> previousTime = new BatchIn<Epoch>(outerBatch, currentSubBatch - 1);
            windowInput.OnDataAtTime(prevThreadWindowInput, previousTime);
            windowInput.OnCompleteTime(previousTime);
            waitStable.Wait();
            waitStable.Reset();
            maxEpoch = epoch - 1;
            if (nonSelective) {
              windowInput.OnDataAtTime(threadWindowInput, currentTime);
            }
          }

          sleepTime = 500 + beginWindow - getCurrentTime();
          if (sleepTime > 0) {
            Thread.Sleep((int)sleepTime);
          }
          windowInput.OnDataAtTime(threadWindowInput, currentTime);
          sleepTime = timeSliceLengthMs + beginWindow - getCurrentTime();
          if (sleepTime > 0) {
            Thread.Sleep((int)sleepTime);
          } else {
            Console.WriteLine("Falling behind by " + (-sleepTime) + " with second input");
          }
          if ((delayEachEpoch == -1 && epoch + 1 != delayAtEpoch) ||
              (delayEachEpoch != -1 && ((epoch + 1) % delayEachEpoch != (delayEachEpoch - 1) || epoch < 20))) {
            windowInput.OnCompleteTime(currentTime);
            waitStable.Wait();
            waitStable.Reset();
            maxEpoch = epoch;
          }
          currentSubBatch++;
          sleepTime = timeSliceLengthMs + beginWindow - getCurrentTime();
          if (sleepTime > 0) {
            Thread.Sleep((int)sleepTime);
          } else {
            Console.WriteLine("Falling behind by " + (-sleepTime) + " with epoch generation");
          }
          beginWindow += timeSliceLengthMs;
        }

        Thread.Sleep(10000);
        windowInput.OnCompleted();

        computation.Join();
        if (computation.Configuration.ProcessID == 0 && enableFT)
        {
          manager.Join();
        }
      }
    }

    public static bool exactlyOnce = false;
    public static int maxEpoch = -1;
    public static CountdownEvent waitStable = null;

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
