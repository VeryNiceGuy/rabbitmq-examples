using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQClient
{
    public abstract class BaseMessage
    {
        public string Guid { get; private set; }
        public BaseMessage(string guid)
        {
            Guid = guid;
        }
    }

    public class NodeScanRequestMessage : BaseMessage
    {
        public string Header { get; set; }
        public string ScannerSerialNumber { get; private set; }
        public string ComputerName { get; private set; }
        public bool ScanEthalon { get; set; }
        public NodeScanRequestMessage(string guid, string scannerSerialNumber, string computerName, string header, bool scanEthalon) : base(guid)
        {
            ScannerSerialNumber = scannerSerialNumber;
            ComputerName = computerName;
            Header = header;
            ScanEthalon = scanEthalon;
        }
    }

    public class NodeScanResponseMessage : BaseMessage
    {
        public string Ethalon { get; set; }
        public string Identity { get; set; }
        public string Message { get; set; }
        public bool OverwriteMessage { get; set; }
        public NodeScanResponseMessage(string guid, string identity, string ethalon) : base(guid)
        {
            Identity = identity;
            Ethalon = ethalon;
        }
        public NodeScanResponseMessage(string guid, string message) : base(guid)
        {
            Message = message;
        }
    }

    public class ConnectedClient { }

    public interface IConnectionsService
    {
        Task<NodeScanResponseMessage> ScanOnLocalClientAsync(ConnectedClient client, NodeScanRequestMessage scanRequest, CancellationToken cancellationToken);
    }

    public class ConnectionsService : IConnectionsService
    {
        public async Task<NodeScanResponseMessage> ScanOnLocalClientAsync(ConnectedClient client, NodeScanRequestMessage scanRequest, CancellationToken cancellationToken)
        {
            return new NodeScanResponseMessage("2", "2", "2");
        }
    }

    public enum MessageType
    {
        QueueRegistrationRequest,
        QueueRegistrationResponse,
        ScanRequest,
        ScanResponse
    }

    public class Message
    {
        public MessageType MessageType { get; set; }
        public string QueueName { get; set; }
        public NodeScanRequestMessage ScanRequest { get; set; }
        public NodeScanResponseMessage ScanResponse { get; set; }
    } 

    public class Client
    {
        private readonly IConnectionsService _connectionService;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly string _queueName;
        private readonly EventingBasicConsumer _consumer;
        private readonly BlockingCollection<Message> _incomingMessageQueue = new BlockingCollection<Message>();
        private readonly IBasicProperties _properties;

        public Client(IConnectionsService connectionService)
        {
            _connectionService = connectionService;
            var factory = new ConnectionFactory() { HostName = "localhost" };
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            _queueName = _channel.QueueDeclare().QueueName;
            _consumer = new EventingBasicConsumer(_channel);
            _properties = _channel.CreateBasicProperties();
            _properties.ReplyTo = _queueName;
            _consumer.Received += (model, eventArgs) =>
            {
                var text = Encoding.UTF8.GetString(eventArgs.Body);
                var message = JsonConvert.DeserializeObject<Message>(text);

                if(message.MessageType == MessageType.QueueRegistrationResponse)
                    _incomingMessageQueue.Add(message);
                else
                {
                    var scanResponse = _connectionService.ScanOnLocalClientAsync(new ConnectedClient(), message.ScanRequest, CancellationToken.None).Result;
                    _channel.BasicPublish("", "rpc_queue", _properties,
                        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new Message{ MessageType = MessageType.ScanResponse, QueueName = message.QueueName, ScanResponse = scanResponse})));
                }
            };

            _channel.BasicPublish("", "rpc_queue", _properties, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new Message{ MessageType = MessageType.QueueRegistrationRequest })));
            _channel.BasicConsume(consumer: _consumer, queue: _queueName, autoAck: true);

            /* Diagnostic purposes only. */
            var response = _incomingMessageQueue.Take();
        }

        public NodeScanResponseMessage SendScanRequest(NodeScanRequestMessage scanRequest)
        {
            _channel.BasicPublish("", "rpc_queue", _properties, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new Message { MessageType = MessageType.ScanRequest,QueueName = _queueName, ScanRequest = scanRequest })));
            return _incomingMessageQueue.Take().ScanResponse;
        }

        public void Close()
        {
            _connection.Close();
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var rpcClient = new Client(new ConnectionsService());
            if (Console.ReadKey().Key == ConsoleKey.Enter)
                rpcClient.SendScanRequest(new NodeScanRequestMessage("1", "1", "1", "1", true));


            Console.ReadKey();
            rpcClient.Close();
        }
    }
}
