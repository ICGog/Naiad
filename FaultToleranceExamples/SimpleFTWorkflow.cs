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
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.BatchEntry;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;

using Microsoft.Research.Naiad.Examples.DifferentialDataflow;

namespace FaultToleranceExamples.SimpleFTWorkflow
{
  public static class ExtensionMethods
  {
    // key-value pairs on the first input are retrieved via the second input.
    public static Stream<Pair<TKey, TValue>, Epoch> SimpleFTWorkflow<TKey, TValue>(
        this Stream<Pair<TKey, TValue>, Epoch> kvpairs,
        Stream<TKey, Epoch> requests)
    {
      var stage = Foundry.NewBinaryStage(kvpairs, requests,
                                         (i, s) => new SimpleFTWorkflowVertex<TKey, TValue>(i, s),
                                         x => x.First.GetHashCode(),
                                         y => y.GetHashCode(), null, "Lookup");
      stage.SetCheckpointType(CheckpointType.StatelessLogEphemeral);
      stage.SetCheckpointPolicy(v => new CheckpointEagerly());
      return stage;
    }

    public class SimpleFTWorkflowVertex<TKey, TValue> : BinaryVertex<Pair<TKey, TValue>, TKey, Pair<TKey, TValue>, Epoch>
    {
      private readonly Dictionary<TKey, TValue> Values =
        new Dictionary<TKey, TValue>();

      public override void OnReceive1(Message<Pair<TKey, TValue>, Epoch> message)
      {
        for (int i = 0; i < message.length; i++)
          this.Values[message.payload[i].First] = message.payload[i].Second;
      }

      public override void OnReceive2(Message<TKey, Epoch> message)
      {
        var output = this.Output.GetBufferForTime(message.time);
        for (int i = 0; i < message.length; i++)
        {
          var key = message.payload[i];
          if (this.Values.ContainsKey(key))
            output.Send(key.PairWith(this.Values[key]));
        }
      }

      public SimpleFTWorkflowVertex(int index, Stage<Epoch> vertex) : base(index, vertex) { }
    }
  }


  public class SimpleFTWorkflow : Example
  {

    private Configuration config;

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
        this.logFile = new FileStream(Path.Combine(prefix, fileName),
                                      FileMode.Create,
                                      FileAccess.Write,
                                      FileShare.ReadWrite);
        this.log = new StreamWriter(this.logFile);
        var flush = new System.Threading.Thread(
            new System.Threading.ThreadStart(() => this.FlushFileThread()));
        flush.Start();
      }
    }

    public string Usage { get { return ""; } }

    public void Execute(string[] args)
    {
      this.config = Configuration.FromArgs(ref args);

      string logPrefix = "";
      int i = 0;
      while (i < args.Length)
      {
        switch (args[i].ToLower())
        {
          case "-log":
            logPrefix = args[i + 1];
            i += 2;
            break;
          default:
            i += 1;
            break;
        }
      }

      System.IO.Directory.CreateDirectory(logPrefix);
      this.config.LogStreamFactory = (s => new FileLogStream(logPrefix, s));
      FTManager manager = new FTManager(this.config.LogStreamFactory, null, null);
      System.IO.Directory.CreateDirectory(Path.Combine(logPrefix, "checkpoint"));
      this.config.CheckpointingFactory = s => new FileStreamSequence(Path.Combine(logPrefix, "checkpoint"), s);
      this.config.DefaultCheckpointInterval = 1000;
      this.config.MaxLatticeInternStaleTimes = 1;

      using (var computation = NewComputation.FromConfig(this.config))
      {
        var keyvals = new BatchedDataSource<Pair<string, string>>();
        var queries = new BatchedDataSource<string>();

        var simpleFTStage = computation.NewInput(keyvals)
          .SimpleFTWorkflow(computation.NewInput(queries));
        simpleFTStage
          .Subscribe(list => { foreach (var l in list) Console.WriteLine("value[\"{0}\"]:\t\"{1}\"", l.First, l.Second); });

        if (this.config.ProcessID == 0)
        {
          manager.Initialize(computation, new int[] {simpleFTStage.ForStage.StageId}, this.config.WorkerCount, true);
        }

        computation.Activate();

        if (computation.Configuration.ProcessID == 0)
        {
          Console.WriteLine("Enter two strings to insert/overwrite a (key, value) pairs.");
          Console.WriteLine("Enter one string to look up a key.");

          // repeatedly read lines and introduce records based on their structure.
          // note: it is important to advance both inputs in order to make progress.
          for (var line = Console.ReadLine(); line.Length > 0; line = Console.ReadLine())
          {
            var split = line.Split();
            if (split.Length == 1)
            {
              queries.OnNext(line);
              keyvals.OnNext();
            }
            if (split.Length == 2)
            {
              queries.OnNext();
              keyvals.OnNext(split[0].PairWith(split[1]));
            }
            if (split.Length > 2)
              Console.Error.WriteLine("error: lines with three or more strings are not understood.");
          }
        }

        keyvals.OnCompleted();
        queries.OnCompleted();

        computation.Join();
      }
    }

    public string Help
    {
      get { return "Interactive get set functionality for a distributed KV store"; }
    }
  }
}