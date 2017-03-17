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
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Peloponnese.Hdfs;

namespace FaultToleranceExamples
{
  internal class HdfsStreamSequence : IStreamSequence
  {
    private HdfsLogAppendStream currentStream;
    private readonly string directory;
    private string streamBaseName;

    public string Name { get { return this.streamBaseName; } }

    public Stream CurrentStream
    {
      get { return this.currentStream; }
    }

    public void Flush(Stream stream)
    {
      HdfsLogAppendStream hStream = stream as HdfsLogAppendStream;
      hStream.Flush();
    }

    public void Shutdown()
    {
      Stream stream = this.CloseCurrentStream();
      if (stream != null)
      {
        this.Close(stream);
      }
    }

    public void Close(Stream stream)
    {
      stream.Flush();
      stream.Close();
      stream.Dispose();
    }

    public Stream CloseCurrentStream()
    {
      Stream stream = this.currentStream;
      this.currentStream = null;
      return stream;
    }

    public void OpenWriteStream(int index)
    {
      Uri logUri = this.StreamUri(index);
      this.currentStream = new HdfsLogAppendStream(logUri, true);
    }

    public Stream OpenReadStream(int index)
    {
      Uri logUri = this.StreamUri(index);
      return new HdfsLogReaderStream(logUri);
    }

    private Uri StreamUri(int index)
    {
      string streamName =
        Path.Combine(this.directory,
                     String.Format("{0}.{1:D4}", this.streamBaseName, index));
      UriBuilder builder = new UriBuilder(streamName);
      return builder.Uri;
    }

    public void GarbageCollectStream(int streamIndex)
    {
      Uri logUri = this.StreamUri(streamIndex);
      HdfsInstance hdfsInstance = new HdfsInstance(logUri);
      if (hdfsInstance.IsFileExists(logUri.AbsolutePath))
      {
        hdfsInstance.DeleteFile(logUri.AbsolutePath, false);
      }
    }

    public IEnumerable<int> FindStreams()
    {
      UriBuilder builder = new UriBuilder(this.directory);
      Uri dirUri = builder.Uri;
      Console.WriteLine("URI {0} {1}", dirUri.Host, dirUri.Port);
      HdfsInstance hdfsInstance = new HdfsInstance(dirUri);
      Console.WriteLine("Post HDFS INSTANCE");
      if (hdfsInstance.IsFileExists(dirUri.AbsolutePath))
      {
        yield return 0;
        // HdfsFileInfo info = hdfsInstance.GetFileInfo(dirUri.AbsolutePath, false);
        // foreach (string path in info.fileNameArray)
        // {
        //   string suffix = path.Split('.').Last();
        //   int index = Int32.Parse(suffix);
        //   yield return index;
        // }
      }
    }

    public HdfsStreamSequence(string directory, string streamBaseName)
    {
      this.directory = directory;
      this.streamBaseName = streamBaseName;
    }

  }
}