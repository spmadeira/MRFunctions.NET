using System;
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
        internal Action<KeyValuePair<TKey, TValue>> Write { get; init; }
        
        private ConcurrentBag<IEnumerable<KeyValuePair<TKey, TValue>>> Groups = new();
        private ConcurrentDictionary<TKey, ConcurrentBag<TValue>> Buckets = new();
        private ConcurrentBag<KeyValuePair<TKey, TValue>> Pairs = new();

        public void Run(TInput input)
        {
            var readData = Read(input);
            
            readData
                .AsParallel()
                .ForAll(MapData);
            
            Groups
                .AsParallel()
                .ForAll(Shuffle);
            
            Buckets
                .AsParallel()
                .ForAll(ReduceBucket);
            
            Pairs
                .AsParallel()
                .ForAll(Write);
        }

        private void MapData(TData data)
        {
            var group = Map(data);
            Groups.Add(group);
        }
        
        private void Shuffle(IEnumerable<KeyValuePair<TKey, TValue>> group)
        {
            group
                .AsParallel()
                .ForAll(pair =>
                {
                    Buckets.AddOrUpdate(pair.Key, (_) => new ConcurrentBag<TValue> {pair.Value}, (_, val) =>
                    {
                        val.Add(pair.Value);
                        return val;
                    });
                });
        }

        private void ReduceBucket(KeyValuePair<TKey, ConcurrentBag<TValue>> bucket)
        {
            var value = Reduce(bucket.Key, bucket.Value.ToArray());
            Pairs.Add(new KeyValuePair<TKey, TValue>(bucket.Key, value));
        }

        private void Cleanup()
        {
            Groups.Clear();
            Buckets.Clear();
            Pairs.Clear();
        }
    }
}