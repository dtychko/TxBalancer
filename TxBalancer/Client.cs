using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TxBalancer
{
    internal class Client
    {
        private readonly IConnection _connection;
        private readonly ushort _outputQueueIndex;
        private readonly ushort _prefetchCount;
        private IModel _model;

        public Client(IConnection connection, ushort outputQueueIndex, ushort prefetchCount)
        {
            _connection = connection;
            _outputQueueIndex = outputQueueIndex;
            _prefetchCount = prefetchCount;
        }

        public void Start()
        {
            _model = _connection.CreateModel();
            _model.BasicQos(0, _prefetchCount, false);
            _model.TxSelect();

            _model.QueueDeclare(Program.OutputQueueName(_outputQueueIndex), true, false, false);

            var consumer = new EventingBasicConsumer(_model);
            consumer.Received += async (_, args) =>
            {
                var messageId = args.BasicProperties.MessageId;
                var deliveryTag = args.DeliveryTag;

                await RabbitMqUtils.InTransaction(_model, model =>
                {
                    model.BasicAck(deliveryTag, false);

                    var properties = model.CreateBasicProperties();
                    properties.MessageId = messageId;
                    properties.Persistent = true;
                    model.BasicPublish("", Program.ResponseQueueName, properties, Array.Empty<byte>());
                });
            };
            _model.BasicConsume(consumer, Program.OutputQueueName(_outputQueueIndex));
        }
    }
}