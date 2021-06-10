using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace TxBalancer
{
    internal static class Program
    {
        public static readonly Uri AmqpUri = new Uri("amqp://guest:guest@localhost:5672/");
        public static readonly string InputQueueName = "_tx_balancer_input";
        public static readonly string ResponseQueueName = "_tx_balancer_response";
        public static readonly Func<int, string> OutputQueueName = i => $"_tx_balancer_output_{i}";
        public static readonly Func<int, string> OutputMirrorQueueName = i => $"{OutputQueueName(i)}.mirror";

        public static async Task Main()
        {
            var connectionFactory = new ConnectionFactory
            {
                Uri = AmqpUri
            };

            using (var connection = connectionFactory.CreateConnection())
            using (var model = connection.CreateModel())
            {
                const ushort queueCount = 3;
                const ushort queueSizeLimit = 100;

                new Balancer(
                    connection,
                    queueCount,
                    queueSizeLimit,
                    queueCount * queueSizeLimit * 2,
                    queueCount * queueSizeLimit * 2
                ).Start();

                for (var i = 0; i < queueCount; i++)
                {
                    new Client(
                        connection,
                        (ushort) (i + 1),
                        queueSizeLimit
                    ).Start();
                }

                new Publisher(
                    connection,
                    1000,
                    16 * 1024
                ).Start();

                Console.ReadKey();
            }
        }
    }

    internal class Publisher
    {
        private readonly IConnection _connection;
        private readonly int _messageCountLimit;
        private readonly int _messageSize;
        private IModel _model;

        public Publisher(IConnection connection, int messageCountLimit, int messageSize)
        {
            _connection = connection;
            _messageCountLimit = messageCountLimit;
            _messageSize = messageSize;
        }

        public void Start()
        {
            _model = _connection.CreateModel();
            _model.TxSelect();

            _model.QueueDeclare(Program.InputQueueName, true, false, false);

            Task.Run(() =>
            {
                var messageCountToSend = _messageCountLimit;

                while (true)
                {
                    if (messageCountToSend > 0)
                    {
                        for (var i = 0; i < messageCountToSend; i++)
                        {
                            var properties = _model.CreateBasicProperties();
                            properties.Persistent = true;
                            _model.BasicPublish("", Program.InputQueueName, properties, new byte[_messageSize]);
                        }

                        _model.TxCommit();
                    }

                    messageCountToSend =
                        _messageCountLimit - (int) _model.QueueDeclarePassive(Program.InputQueueName).MessageCount;
                }
            });
        }
    }
}