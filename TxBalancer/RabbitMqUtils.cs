using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace TxBalancer
{
    internal static class RabbitMqUtils
    {
        private static readonly ConcurrentDictionary<IModel, Task> tasks =
            new ConcurrentDictionary<IModel, Task>();

        private static readonly
            ConcurrentDictionary<IModel, ActionQueue> actionQueues = new ConcurrentDictionary<IModel, ActionQueue>();

        private class ActionQueue
        {
            public TaskCompletionSource<bool> TaskCompletionSource { get; } = new TaskCompletionSource<bool>();

            public ConcurrentQueue<Action<IModel>> Queue { get; } = new ConcurrentQueue<Action<IModel>>();
        }

        public static Task InTransaction(IModel model, Action<IModel> action)
        {
            lock (tasks)
            {
                if (tasks.TryGetValue(model, out _))
                {
                    var actionQueue = actionQueues.GetOrAdd(model, _ => new ActionQueue());
                    actionQueue.Queue.Enqueue(action);
                    return actionQueue.TaskCompletionSource.Task;
                }

                var task = Task.Run(() =>
                {
                    action(model);
                    model.TxCommit();
                });
                tasks.TryAdd(model, task);
                ScheduleContinuation(task);
                return task;
            }

            void ScheduleContinuation(Task t)
            {
                t.ContinueWith(__ =>
                {
                    lock (tasks)
                    {
                        tasks.TryRemove(model, out _);
                        if (!actionQueues.TryRemove(model, out var actionQueue))
                        {
                            return;
                        }

                        var task = Task.Run(() =>
                        {
                            while (actionQueue.Queue.TryDequeue(out var delayedAction))
                            {
                                delayedAction(model);
                            }

                            model.TxCommit();
                            actionQueue.TaskCompletionSource.SetResult(true);
                        });
                        tasks.TryAdd(model, task);
                        ScheduleContinuation(task);
                    }
                });
            }
        }
    }
}