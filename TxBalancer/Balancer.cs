using System;
using System.Collections.Concurrent;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TxBalancer
{
    internal class Balancer
    {
        private readonly ConcurrentDictionary<string, ulong> _messageDeliveryTagByMessageId =
            new ConcurrentDictionary<string, ulong>();

        private readonly ConcurrentDictionary<string, ulong> _responseDeliveryTagByMessageId =
            new ConcurrentDictionary<string, ulong>();

        private readonly IConnection _connection;
        private readonly ushort _queueCount;
        private readonly ushort _queueSizeLimit;
        private readonly ushort _outputPrefetchCount;
        private readonly ushort _inputPrefetchCount;
        private IModel _outputModel;
        private IModel _inputModel;

        public Balancer(IConnection connection, ushort queueCount, ushort queueSizeLimit, ushort outputPrefetchCount,
            ushort inputPrefetchCount)
        {
            _connection = connection;
            _queueCount = queueCount;
            _queueSizeLimit = queueSizeLimit;
            _outputPrefetchCount = outputPrefetchCount;
            _inputPrefetchCount = inputPrefetchCount;
        }

        public void Start()
        {
            _outputModel = _connection.CreateModel();
            _outputModel.BasicQos(0, _outputPrefetchCount, false);
            _outputModel.TxSelect();

            _inputModel = _connection.CreateModel();
            _inputModel.BasicQos(0, _inputPrefetchCount, false);
            _inputModel.TxSelect();

            DeclareResources();

            ConsumeMirrorQueues();
            ConsumeResponseQueue();
            ConsumeInputQueue();
        }

        private void DeclareResources()
        {
            DeclareAndPurgeQueue(Program.InputQueueName);
            DeclareAndPurgeQueue(Program.ResponseQueueName);

            for (var i = 0; i < _queueCount; i++)
            {
                DeclareAndPurgeQueue(Program.OutputQueueName(i + 1));
                DeclareAndPurgeQueue(Program.OutputMirrorQueueName(i + 1));
            }

            void DeclareAndPurgeQueue(string queueName)
            {
                _outputModel.QueueDeclare(queueName, true, false, false);
                _outputModel.QueuePurge(queueName);
            }
        }

        private void ConsumeMirrorQueues()
        {
            for (var i = 0; i < _queueCount; i++)
            {
                var queueName = Program.OutputMirrorQueueName(i + 1);
                var consumer = new EventingBasicConsumer(_outputModel);
                consumer.Received += async (_, args) =>
                {
                    var messageId = args.BasicProperties.MessageId;
                    var deliveryTag = args.DeliveryTag;

                    if (_responseDeliveryTagByMessageId.TryRemove(messageId, out var responseDeliveryTag))
                    {
                        await RabbitMqUtils.InTransaction(_outputModel, model =>
                        {
                            model.BasicAck(deliveryTag, false);
                            model.BasicAck(responseDeliveryTag, false);
                        });
                        OnMessageProcessed();
                        return;
                    }

                    _messageDeliveryTagByMessageId.TryAdd(messageId, deliveryTag);
                };
                _outputModel.BasicConsume(consumer, queueName);
            }
        }

        private void ConsumeResponseQueue()
        {
            var consumer = new EventingBasicConsumer(_outputModel);
            consumer.Received += async (_, args) =>
            {
                var messageId = args.BasicProperties.MessageId;
                var deliveryTag = args.DeliveryTag;

                if (_messageDeliveryTagByMessageId.TryRemove(messageId, out var messageDeliveryTag))
                {
                    await RabbitMqUtils.InTransaction(_outputModel, model =>
                    {
                        model.BasicAck(messageDeliveryTag, false);
                        model.BasicAck(deliveryTag, false);
                    });
                    OnMessageProcessed();
                    return;
                }

                _responseDeliveryTagByMessageId.TryAdd(messageId, deliveryTag);
            };
            _outputModel.BasicConsume(consumer, Program.ResponseQueueName);
        }

        private void ConsumeInputQueue()
        {
            var consumer = new EventingBasicConsumer(_inputModel);
            consumer.Received += async (_, args) =>
            {
                var messageId = Guid.NewGuid().ToString("D");
                var deliveryTag = args.DeliveryTag;
                var outputQueueIndex = deliveryTag % _queueCount + 1;

                var spinWait = new SpinWait();
                while (_processingMessages >= _queueCount * _queueSizeLimit)
                {
                    spinWait.SpinOnce();
                }

                OnMessageProcessing();

                await RabbitMqUtils.InTransaction(_inputModel, model =>
                {
                    model.BasicAck(deliveryTag, false);

                    var properties = model.CreateBasicProperties();
                    properties.MessageId = messageId;
                    properties.Persistent = true;
                    model.BasicPublish("", Program.OutputQueueName((int) outputQueueIndex), properties, args.Body);

                    properties = model.CreateBasicProperties();
                    properties.MessageId = messageId;
                    properties.Persistent = true;
                    model.BasicPublish("", Program.OutputMirrorQueueName((int) outputQueueIndex), properties,
                        args.Body);
                });
            };
            _inputModel.BasicConsume(consumer, Program.InputQueueName);
        }

        private volatile int _processingMessages;
        private volatile int _processedMessages;

        private void OnMessageProcessing()
        {
            Interlocked.Increment(ref _processingMessages);
        }

        private void OnMessageProcessed()
        {
            Interlocked.Decrement(ref _processingMessages);
            if (Interlocked.Increment(ref _processedMessages) % 10000 == 0)
            {
                Console.WriteLine($"[Balancer] Processed {_processedMessages} messages");
            }
        }
    }
}