using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Collections.Generic;

class Receiver
{
    public static void Main()
    {
        // run_cmd executes a python file with given arguments
        string run_cmd(string cmd, string args2)
        {
            // simple delay of 1 second
            Thread.Sleep(1000);
            ProcessStartInfo start = new ProcessStartInfo();

            // Python executable file path
            start.FileName = "C:\\Users\\inviter\\virtualenv\\aemenenv36\\Scripts\\python.exe";

            // cmd is the path for python file and args2 represents the json file path
            start.Arguments = string.Format("\"{0}\" \"{1}\"", cmd, args2);
            start.UseShellExecute = false;
            start.CreateNoWindow = true;
            start.RedirectStandardOutput = true;
            start.RedirectStandardError = true;
            using (Process process = Process.Start(start))
            {
                using (StreamReader reader = process.StandardOutput)
                {
                    string stderr = process.StandardError.ReadToEnd();
                    string result = reader.ReadToEnd();
                    return stderr;
                }
            }
        }

        // CopyHeaders gets the headers from the message
        IDictionary<string, object> CopyHeaders(IBasicProperties originalProperties)
        {
            IDictionary<string, object> dict = new Dictionary<string, object>();
            IDictionary<string, object> headers = originalProperties.Headers;
            if (headers != null)
            {
                foreach (KeyValuePair<string, object> kvp in headers)
                {
                    dict[kvp.Key] = kvp.Value;
                }
            }
            return dict;
        }

        //GetRetryCount gets the retry count stored in the headers of the message
        int GetRetryCount(IBasicProperties messageProperties, string countHeader)
        {
            IDictionary<string, object> headers = messageProperties.Headers;
            int count = 0;
            if (headers != null)
            {
                if (headers.ContainsKey(countHeader))
                {
                    string countAsString = Convert.ToString(headers[countHeader]);
                    count = Convert.ToInt32(countAsString);
                }
            }

            return count;
        }

        void ReceiveMessage(IModel model)
        {
            // arg1: whether to send next message until the previous one is acknowledged
            // arg2: prefetch count set to 1
            model.BasicQos(0, 1, false);
            QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
            
            model.BasicConsume("test_queue", false, consumer);
            string customRetryHeaderName = "number-of-retries";
            int maxNumberOfRetries = 3;
            while (true)
            {
                BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
                String message = Encoding.UTF8.GetString(deliveryArguments.Body);
                Console.WriteLine("Message from queue: {0}", message);
                Random random = new Random();
                int retryCount = GetRetryCount(deliveryArguments.BasicProperties, customRetryHeaderName);
                try
                {
                    Console.WriteLine(deliveryArguments.BasicProperties.MessageId);
                    JObject obj = JObject.Parse(message);

                    var result_ = run_cmd((String)obj["cmd"], (String)obj["jsonPath"]);
                    Console.WriteLine(result_);
                    // acknowledges the message on successful execution
                    model.BasicAck(deliveryArguments.DeliveryTag, false);
                }
                catch
                {
                    // checks retry count and republishes message if retry count less than the max_retries
                    if (retryCount < maxNumberOfRetries)
                    {
                        Console.WriteLine("Message {0} has thrown an exception. Current number of retries: {1}", message, retryCount);
                        IBasicProperties propertiesForCopy = model.CreateBasicProperties();
                        IDictionary<string, object> headersCopy = CopyHeaders(deliveryArguments.BasicProperties);
                        propertiesForCopy.Headers = headersCopy;
                        propertiesForCopy.Headers[customRetryHeaderName] = ++retryCount;
                        model.BasicPublish(deliveryArguments.Exchange, deliveryArguments.RoutingKey, propertiesForCopy, deliveryArguments.Body);
                        model.BasicAck(deliveryArguments.DeliveryTag, false);
                        Console.WriteLine("Message {0} thrown back at queue for retry. New retry count: {1}", message, retryCount);
                    }
                    else
                    {
                        Console.WriteLine("Message {0} has reached the max number of retries. It will be rejected.", message);
                        model.BasicReject(deliveryArguments.DeliveryTag, false);
                    }
                }
            }
        }


        var factory = new ConnectionFactory() { HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "test_queue",
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.ExchangeDeclare(exchange: "aemen_direct", type: "direct", durable: true);

            channel.QueueBind(queue: "test_queue",
                  exchange: "aemen_direct",
                  routingKey: "aemen_direct");
            ReceiveMessage(channel);
        }
    }
}