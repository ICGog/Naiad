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
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad;

namespace FaultToleranceExamples.YCSB
{
  public class KafkaConsumer
  {
    private Consumer<Null, string> consumer;

    public KafkaConsumer(string brokerList, string topic)
    {
      var config = new Dictionary<string, object>
        {
          { "bootstrap.servers", brokerList },
          { "group.id", "myGroup" },
          { "auto.offset.reset", "earliest" }
        };
      this.consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8));
      consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, 0)});
    }

    public void StartConsumer(BatchedDataSource<string> kafkaInput, Computation computation)
    {
      int i = 0;
      int epoch = 0;
      List<string> input = new List<string>();
      while (true)
      {
        Message<Null, string> msg;
        if (consumer.Consume(out msg, TimeSpan.FromMilliseconds(100)))
        {
          i++;
          if (i % 1000 == 0)
          {
            kafkaInput.OnNext(input);
            computation.Sync(epoch);
            input.Clear();
            epoch++;
          }
          input.Add(msg.Value);
        }
      }
    }
  }
}