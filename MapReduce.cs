﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MRFunctions
{
    public abstract class MapReduce
    {
        public static MRBuilder<TInput> WithInput<TInput>()
        {
            return new();
        }
    }

    public class MapReduce<
        TInput,
        TData,
        TKey,
        TValue>
    {
        internal Func<TInput, IEnumerable<TData>> Read { get; init; }
        internal Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map { get; init; }
        internal Func<TKey, TKey, bool> Compare { get; init; }
        internal Func<TKey, IEnumerable<TValue>, TValue> Reduce { get; init; }
        internal Func<KeyValuePair<TKey, TValue>, Task> Write { get; init; }

        private IEnumerable<TData> Data;
        private ConcurrentBag<IEnumerable<KeyValuePair<TKey, TValue>>> Groups;
        private ConcurrentDictionary<TKey, ConcurrentBag<TValue>> Buckets;
        private ConcurrentBag<KeyValuePair<TKey, TValue>> Pairs;

        public async Task Run(TInput input)
        {
            Data = await Task.Run(() => Read(input));
            
            Groups = new ConcurrentBag<IEnumerable<KeyValuePair<TKey, TValue>>>();

            var mapOperations = Data
                .Select(MapData);

            await Task.WhenAll(mapOperations);

            Buckets = new ConcurrentDictionary<TKey, ConcurrentBag<TValue>>();
            
            var shuffleOperations = Groups
                .Select(Shuffle);

            await Task.WhenAll(shuffleOperations);

            Pairs = new ConcurrentBag<KeyValuePair<TKey, TValue>>();

            var reductionOperations = Buckets
                .Select(ReduceBucket);

            await Task.WhenAll(reductionOperations);

            var writeOperations = Pairs
                .Select(Write);

            await Task.WhenAll(writeOperations);
        }

        private Task MapData(TData data)
        {
            return Task.Run(() =>
            {
                var group = Map(data);
                Groups.Add(group);
            });
        }
        
        private Task Shuffle(IEnumerable<KeyValuePair<TKey, TValue>> group)
        {
            var tasks = group.Select(pair =>
            {
                return Task.Run(() =>
                {
                    Buckets.AddOrUpdate(pair.Key, new ConcurrentBag<TValue> {pair.Value}, (_, val) =>
                    {
                        val.Add(pair.Value);
                        return val;
                    });
                });
            });

            return Task.WhenAll(tasks);
        }

        private Task ReduceBucket(KeyValuePair<TKey, ConcurrentBag<TValue>> bucket)
        {
            return Task.Run(() =>
            {
                var value = Reduce(bucket.Key, bucket.Value.ToArray());
                Pairs.Add(new KeyValuePair<TKey, TValue>(bucket.Key, value));
            });
        }
    }
}