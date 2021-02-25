using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MRFunctions
{
    public class MRBuilder<TInput>
    {
        public MRBuilder<TInput, TData> WithReader<TData>(Func<TInput, IEnumerable<TData>> readFunction)
        {
            return new()
            {
                Read = readFunction
            };
        }
    }

    public class MRBuilder<TInput, TData> : MRBuilder<TInput>
    {
        public Func<TInput, IEnumerable<TData>> Read { get; init; }

        public MRBuilder<TInput, TData, TKey, TValue> WithMapper<TKey, TValue>(
            Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> mapper)
        {
            return new()
            {
                Read = Read,
                Map = mapper
            };
        }
        
    }

    public class MRBuilder<TInput, TData, TKey, TValue> : MRBuilder<TInput, TData>
    {
        internal Func<TData, IEnumerable<KeyValuePair<TKey, TValue>>> Map { get; init; }
        
        internal Func<TKey, TKey, bool> Compare { get; set; } = (k1, k2) => k1.Equals(k2);
        
        internal Func<TKey, IEnumerable<TValue>, TValue> Reduce { get; set; }

        internal Func<KeyValuePair<TKey, TValue>, Task> Write { get; set; }
            = (pair) => Task.Run(() => Console.WriteLine($"Key: {pair.Key} | Value: {pair.Value}"));

        public MRBuilder<TInput, TData, TKey, TValue> WithComparer(Func<TKey, TKey, bool> comparer)
        {
            Compare = comparer;
            return this;
        }
        
        public MRBuilder<TInput, TData, TKey, TValue> WithReducer(Func<TKey, IEnumerable<TValue>, TValue> reducer)
        {
            Reduce = reducer;
            return this;
        }

        public MRBuilder<TInput, TData, TKey, TValue> WithWriter(Func<KeyValuePair<TKey, TValue>, Task> writer)
        {
            Write = writer;
            return this;
        }
        
        public MRBuilder<TInput, TData, TKey, TValue> WithWriter(Action<KeyValuePair<TKey, TValue>> writer)
        {
            Write = pair => Task.Run(() => writer(pair));
            return this;
        }

        public MapReduce<TInput, TData, TKey, TValue> Build()
        {
            if (Reduce == null)
                throw new ArgumentException("Reducer cannot be null", nameof(Reduce));
            
            return new()
            {
                Read = Read,
                Map = Map,
                Compare = Compare,
                Reduce = Reduce,
                Write = Write
            };
        }

        public static implicit operator MapReduce<TInput, TData, TKey, TValue>(MRBuilder<TInput, TData, TKey, TValue> builder)
        {
            return builder.Build();
        }
    }
}