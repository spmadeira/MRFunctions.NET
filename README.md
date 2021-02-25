# MRFunctions.NET

Small library for simulating a MapReduce operation, written with C#.

### Usage


```csharp
var text = "Deer Bear River\nCar Car River\nDeer Car Bear";

    await MapReduce
        .WithInput<string>()
        .WithReader(input => input.Split("\n"))
        .WithMapper(data =>
        {
            var keys = data.Split(" ");
            return keys.Select(k => new KeyValuePair<string, int>(k, 1));
        })
        .WithReducer((word, instances) => instances.Sum())
        .WithWriter(pair => System.Console.WriteLine($"Key: {pair.Key} | Value: {pair.Value}"))
        .Build().Run(text);
```
Output:
```
Key: River | Value: 2
Key: Car | Value: 3
Key: Deer | Value: 2
Key: Bear | Value: 2
```
