using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TxBalancer
{
    internal static class Program
    {
        public static readonly string InputQueueName = "input";
        public static readonly string ResponseQueueName = "response";
        public static readonly Func<int, string> OutputQueueName = i => $"output_{i}";
        public static readonly Func<int, string> OutputMirrorQueueName = i => $"{OutputQueueName(i)}.mirror";

        public static void Main(string[] args)
        {
        }
    }

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
                consumer.Received += (_, args) =>
                {
                    var messageId = args.BasicProperties.MessageId;
                    var deliveryTag = args.DeliveryTag;

                    if (_responseDeliveryTagByMessageId.TryRemove(messageId, out var responseDeliveryTag))
                    {
                        // TODO: ack 2 messages
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
            consumer.Received += (_, args) =>
            {
                var messageId = args.BasicProperties.MessageId;
                var deliveryTag = args.DeliveryTag;

                if (_messageDeliveryTagByMessageId.TryRemove(messageId, out var messageDeliveryTag))
                {
                    // TODO: ack 2 messages
                    OnMessageProcessed();
                    return;
                }

                _responseDeliveryTagByMessageId.TryAdd(messageId, deliveryTag);
            };
            _outputModel.BasicConsume(consumer, Program.ResponseQueueName);
        }

        // private static readonly ConcurrentDictionary<IModel, Task> tasks =
        //     new ConcurrentDictionary<IModel, Task>();
        //
        // private static readonly
        //     ConcurrentDictionary<IModel, ConcurrentQueue<(Action<IModel>, TaskCompletionSource<bool>)>> actions =
        //         new ConcurrentDictionary<IModel, ConcurrentQueue<(Action<IModel>, TaskCompletionSource<bool>)>>();
        //
        // private static Task InTransaction(IModel model, Action<IModel> action)
        // {
        //     if (tasks.TryGetValue(model, out var _))
        //     {
        //         var actionQueue = actions.GetOrAdd(model,
        //             _ => new ConcurrentQueue<(Action<IModel>, TaskCompletionSource<bool>)>());
        //         var tcs = new TaskCompletionSource<bool>();
        //         actionQueue.Enqueue((action, tcs));
        //         return tcs.Task;
        //     }
        //
        //     var task = Task.Run(() =>
        //     {
        //         action(model);
        //         model.TxCommit();
        //     });
        //     task.ContinueWith(async __ =>
        //     {
        //         tasks.TryRemove(model, out _);
        //         if (actions.TryRemove(model, out var actionQueue))
        //         {
        //             await InTransaction(model, ___ =>
        //             {
        //                 foreach (var tuple in actionQueue)
        //                 {
        //                     tuple.Item1(model);
        //                 }
        //             });
        //
        //             while (actionQueue.TryDequeue(out var tuple))
        //             {
        //                 tuple.Item2.SetResult(true);
        //             }
        //         }
        //     });
        //     tasks.TryAdd(model, task);
        //     return task;
        // }

        private void ConsumeInputQueue()
        {
            var consumer = new EventingBasicConsumer(_inputModel);
            consumer.Received += (_, args) =>
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

                // TODO: Ack + publish

                // const messageId = generateMessageId();
                // const deliveryTag = msg.fields.deliveryTag;
                // const outputQueueIndex = deliveryTag % queueCount + 1;
                //
                // while (processingMessageCount >= queueCount * queueSizeLimit) {
                //     await setTimeoutAsync(0);
                // }
                //
                // onMessageProcessing();
                //
                // await txCommitSchedule(ch, () => {
                //     ch.ack(msg);
                //     ch.publish('', outputQueueName(outputQueueIndex), msg.content, {persistent: true, messageId});
                //     ch.publish('', outputMirrorQueueName(outputQueueIndex), Buffer.from(''), {persistent: true, messageId});
                // });
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