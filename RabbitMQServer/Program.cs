using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQServer
{
    class Program
    {
        static List<string> _queueNames = new List<string>();
        public static void Main()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.BasicQos(0, 1, false);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, eventArgs) =>
                {
                    var body = eventArgs.Body;
                    var requestProperties = eventArgs.BasicProperties;
                    var responseProperties = channel.CreateBasicProperties();

                    var message = JObject.Parse(Encoding.UTF8.GetString(body));
                    var messageType = message.GetValue("MessageType").ToObject<int>();

                    switch (messageType)
                    {
                        case 0:
                        {
                            channel.BasicPublish("", requestProperties.ReplyTo, responseProperties,
                                Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new Dictionary<string, string> { { "MessageType", "1" } })));
                            _queueNames.Add(requestProperties.ReplyTo);
                            channel.BasicAck(eventArgs.DeliveryTag, false);
                            break;
                        }
                        case 2:
                        {
                            foreach (var name in _queueNames.Where(n => n != requestProperties.ReplyTo))
                                channel.BasicPublish("", name, responseProperties, body);
                            channel.BasicAck(eventArgs.DeliveryTag, false);
                            break;
                        }
                        case 3:
                        {
                            channel.BasicPublish("", message.GetValue("QueueName").ToString(), responseProperties, body);
                            channel.BasicAck(eventArgs.DeliveryTag, false);
                            break;
                        }
                    }
                };

                channel.BasicConsume("rpc_queue", false, consumer);
                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
