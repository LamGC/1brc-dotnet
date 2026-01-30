// #define ENABLE_STOPWATCH
// #define ENABLE_EXPANSION_LOG
// #define ENABLE_HANDLED_ENTRY_COUNTER
// #define ENABLE_WORKER_DEBUG_LOG
// #define USE_ONCE_WORKER
// #define DISABLE_RESULT_OUTPUT

using System.Collections.Immutable;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

#if ENABLE_STOPWATCH
using System.Diagnostics;
#endif

#if ENABLE_STOPWATCH
var stopwatch = Stopwatch.StartNew();
#endif

const string filePath = "D:/1brc/measurements.txt";
var resultLock = new Lock();
var totalResults = new Dictionary<string, DataEntry>();

var fi = new FileInfo(filePath);
var fileSize = fi.Length;

using var mmf = MemoryMappedFile.CreateFromFile(
    filePath, FileMode.Open, "DataSource", 0, MemoryMappedFileAccess.Read);

#if USE_ONCE_WORKER
const int coreCount = 1;
#else
var coreCount = Environment.ProcessorCount;
#endif
var chunkSizePerWorker = fileSize / coreCount;

#if ENABLE_HANDLED_ENTRY_COUNTER
var handledEntryCount = 0;
#endif

Parallel.For(0, coreCount, (i, _) =>
{
    // ReSharper disable once AccessToDisposedClosure
    DoWork(i, chunkSizePerWorker, mmf);
});

var sortedSet = totalResults.Keys.ToImmutableSortedSet();
#if !DISABLE_RESULT_OUTPUT
Console.Write('{');
#endif
var first = true;
for (var index = 0; index < sortedSet.Count; index++)
{
    var key = sortedSet[index];
    var value = totalResults[key];
    if (value.Count == 0)
    {
        continue;
    }

    var avg = value.Total / value.Count;
    if (!first)
    {
#if !DISABLE_RESULT_OUTPUT
        Console.Write(", ");
#endif
    }

#if !DISABLE_RESULT_OUTPUT
    Console.Write($"{key}={value.Min / 10.0:F1}/{avg / 10.0:F1}/{value.Max / 10.0:F1}");
#endif
    first = false;
}

#if ENABLE_STOPWATCH
stopwatch.Stop();
Console.Write('\n');
#if ENABLE_HANDLED_ENTRY_COUNTER
Console.Error.WriteLine($"Warning! When the processing entry counter is enabled, the execution time is unreliable!");
#endif
Console.WriteLine($"Execution Time: {stopwatch.Elapsed}ms.");
#endif

#if ENABLE_HANDLED_ENTRY_COUNTER
Console.WriteLine($"Handled Entry Count: {handledEntryCount}");
#endif
return;

void DoWork(int i, long chunkSize, MemoryMappedFile mappedFile)
{
    var localResult = new FastDictionary();
    unsafe
    {
        var startOffset = i * chunkSize;
        var sizeToMap = Math.Min(chunkSize + 256, fileSize - startOffset);
#if ENABLE_WORKER_DEBUG_LOG
        Console.WriteLine($"Worker {i} Started at {startOffset} - {startOffset + chunkSize} - {sizeToMap}");
#endif
        
        using var accessor = mappedFile.CreateViewAccessor(startOffset, sizeToMap, MemoryMappedFileAccess.Read);
        byte* basePtr = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
        basePtr += accessor.PointerOffset;

        var currentPtr = basePtr;
        byte* endOfLogicalChunk;
        var endOfView = basePtr + sizeToMap;
        if (i == coreCount - 1) 
        {
            endOfLogicalChunk = endOfView; 
        }
        else 
        {
            endOfLogicalChunk = basePtr + chunkSize;
        }

        if (i > 0)
        {
            while (currentPtr < endOfLogicalChunk && *currentPtr != (byte)'\n')
            {
                currentPtr++;
            }

            if (currentPtr >= endOfView)
            {
#if ENABLE_WORKER_DEBUG_LOG
                Console.WriteLine($"Worker {i} Ended on {(long)currentPtr:X}: End Of View on Starting");
#endif
                return;
            }
            currentPtr++;
        }

        var buffer = stackalloc byte[168];
        var bufferIndex = 0;
        var valueIndex = 0;
        var startsOfRound = currentPtr;
        while (currentPtr < endOfView)
        {
            try
            {
                if (*currentPtr == (byte)'\n')
                {
                    // 处理一行.
                    var valueStarts = bufferIndex;
                    var value = long.Parse(Encoding.UTF8.GetString(new ReadOnlySpan<byte>(buffer + valueStarts,
                        valueIndex - valueStarts)), CultureInfo.InvariantCulture);
                    localResult.UpdateOrAdd(startsOfRound, bufferIndex, value);

                    if (currentPtr >= endOfLogicalChunk)
                    {
#if ENABLE_WORKER_DEBUG_LOG
                        Console.WriteLine($"Worker {i} Ended on {(long)currentPtr:N0}: End of Chunk");
#endif
#if ENABLE_HANDLED_ENTRY_COUNTER
                        Interlocked.Increment(ref handledEntryCount);
#endif
                        break;
                    }

                    // 重置状态准备处理下一行.
                    bufferIndex = 0;
                    valueIndex = 0;
                    startsOfRound = currentPtr + 1;
#if ENABLE_HANDLED_ENTRY_COUNTER
                    Interlocked.Increment(ref handledEntryCount);
#endif
                    continue;
                }

                if (*currentPtr == (byte)';')
                {
                    valueIndex = bufferIndex;
                    continue;
                }

                if (valueIndex == 0)
                {
                    buffer[bufferIndex++] = *currentPtr;
                }
                else
                {
                    if (*currentPtr != (byte)'.')
                    {
                        buffer[valueIndex++] = *currentPtr;
                    }
                }
            }
            finally
            {
                currentPtr++;
            }
        }
        
#if ENABLE_WORKER_DEBUG_LOG
        if (currentPtr > endOfView)
        {
            Console.WriteLine($"Worker {i} Ended on {(long)currentPtr:N0}: End Of View");
        }
#endif
        
#if ENABLE_WORKER_DEBUG_LOG
        Console.WriteLine($"Worker {i} Actual Ended at {(long)currentPtr:N0} (Actual Read {currentPtr - basePtr:N0}/{chunkSize}({currentPtr - basePtr - chunkSize}), OverRead {currentPtr - endOfLogicalChunk:N0})");
#endif

        lock (resultLock)
        {
            for (var j = 0; j < localResult.Entries.Length; j++)
            {
                ref var entry = ref localResult.Entries[j];
                if (entry.KeyLength == 0)
                {
                    continue;
                }

                var keyString = Encoding.UTF8.GetString(entry.KeyOffset, entry.KeyLength);
                ref var globalEntry =
                    ref CollectionsMarshal.GetValueRefOrAddDefault(totalResults, keyString, out var exists);

                globalEntry.Count += entry.Count;
                globalEntry.Total += entry.Total;
                globalEntry.Max = exists ? Math.Max(globalEntry.Max, entry.Max) : entry.Max;
                globalEntry.Min = exists ? Math.Min(globalEntry.Min, entry.Min) : entry.Min;
            }
        }
        
        accessor.SafeMemoryMappedViewHandle.ReleasePointer();
    }
}

internal struct DataEntry
{
    public long Total;
    public long Count;
    public long Min;
    public long Max;
}

internal unsafe struct FastDataEntry
{
    public uint Hash;
    public byte* KeyOffset;
    public int KeyLength;
    public long Total;
    public long Count;
    public long Min;
    public long Max;
}

internal unsafe class FastDictionary
{
    #if USE_ONCE_WORKER
    public FastDataEntry[] Entries = new FastDataEntry[30000];
    #else
    public FastDataEntry[] Entries = new FastDataEntry[1024];
    #endif
    public int Count;

#if ENABLE_EXPANSION_LOG
    private int _expansionCount;
#endif

    public ref FastDataEntry GetOrAddDefault(byte* key, int keyLength, out bool exists)
    {
        var hash = FastHash(key, keyLength);
        ref var entry = ref GetOrAddDefault0(Entries, hash, key, keyLength, out var flag);
        if (flag == 2)
        {
            Expansion();
            entry = ref GetOrAddDefault0(Entries, hash, key, keyLength, out flag);
        }

        exists = flag == 0;
        if (!exists)
        {
            Count++;
#if !USE_ONCE_WORKER
            if (Count >= Entries.Length * 0.5)
            {
                Expansion();
            }
#endif
        }

        return ref entry;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe bool CompareKey(byte* a, byte* b, int length)
    {
        // ---------------------------------------------------------
        // 第一梯队：最常见的长度区间 (8 到 16 字节)
        // 1BRC 中大量的城市名落在这里，例如 "San Francisco" (13)
        // ---------------------------------------------------------
        if (length >= 8)
        {
            // 1. 先比头 8 个字节 (把指针当 ulong 读)
            if (*(ulong*)a != *(ulong*)b) return false;

            // 2. 再比尾 8 个字节 (Overlapping 重叠读取)
            // 哪怕 length 是 8，这行代码也是安全的（原地重叠）
            if (*(ulong*)(a + length - 8) != *(ulong*)(b + length - 8)) return false;

            // 3. 如果长度 <= 16，上面两步已经覆盖了所有字节，直接返回 true
            // 这是最快路径！
            if (length <= 16) return true;

            // ---------------------------------------------------------
            // 第二梯队：长 Key (> 16 字节)
            // 比如 "City of ...", "North ...", 或者 100 字节的长串
            // ---------------------------------------------------------
            return CompareKeyLong(a, b, length);
        }

        // ---------------------------------------------------------
        // 第三梯队：短 Key (< 8 字节)
        // ---------------------------------------------------------
        
        // 如果 >= 4 (即 4, 5, 6, 7)
        if (length >= 4)
        {
            // 同样的逻辑：比头4字节(int)，比尾4字节(int)
            return (*(uint*)a == *(uint*)b) && 
                   (*(uint*)(a + length - 4) == *(uint*)(b + length - 4));
        }

        // 极短 Key (0, 1, 2, 3)
        // 这种太短了，直接暴力比字节，或者用 switch
        if (length > 0)
        {
            if (*a != *b) return false;
            if (length > 1)
            {
                if (*(ushort*)(a + length - 2) != *(ushort*)(b + length - 2)) return false;
            }
        }
        
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CompareKeyLong(byte* a, byte* b, int length)
    {
        var cursor = a + 8;
        var target = b + 8;
        var end = a + length - 8;

        while (cursor < end)
        {
            if (*(ulong*)cursor != *(ulong*)target) return false;
            cursor += 8;
            target += 8;
        }
        
        return true;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ref FastDataEntry GetOrAddDefault0(FastDataEntry[] entries, uint hash, byte* keyOffset, int keyLength,
        out int flag)
    {
        var index = hash & (entries.Length - 1);

        if (entries[index].Hash == 0)
        {
            flag = 1;
            entries[index].Hash = hash;
            entries[index].KeyOffset = keyOffset;
            entries[index].KeyLength = keyLength;
            return ref entries[index];
        }

        if (entries[index].Hash == hash 
            && entries[index].KeyLength == keyLength 
            && CompareKey(entries[index].KeyOffset, keyOffset, keyLength))
        {
            flag = 0;
            return ref entries[index];
        }

        var i = 0;
        do
        {
            index = (index + 1) & (entries.Length - 1);
            if (entries[index].Hash == 0)
            {
                flag = 1;
                entries[index].Hash = hash;
                entries[index].KeyOffset = keyOffset;
                entries[index].KeyLength = keyLength;
                return ref entries[index];
            }

            if (entries[index].Hash == hash 
                && entries[index].KeyLength == keyLength 
                && CompareKey(entries[index].KeyOffset, keyOffset, keyLength))
            {
                flag = 0;
                return ref entries[index];
            }

            i++;
        } while (i < entries.Length);

        flag = 2;
        return ref entries[index];
    }

    public void UpdateOrAdd(byte* key, int keyLength, long data)
    {
        ref var entry = ref GetOrAddDefault(key, keyLength, out var exists);

        entry.Count++;
        entry.Total += data;
        if (!exists || entry.Max < data)
        {
            entry.Max = data;
        }

        if (!exists || entry.Min > data)
        {
            entry.Min = data;
        }
    }

    private void Expansion()
    {
        var newEntries = new FastDataEntry[Entries.Length * 2];

        for (var i = 0; i < Entries.Length; i++)
        {
            ref var newSlot = ref GetOrAddDefault0(newEntries, Entries[i].Hash, Entries[i].KeyOffset,
                Entries[i].KeyLength, out _);
            newSlot.Total += Entries[i].Total;
            newSlot.Count = Entries[i].Count;
            newSlot.Min = Entries[i].Min;
            newSlot.Max = Entries[i].Max;
        }

#if ENABLE_EXPANSION_LOG
        Console.WriteLine($"Expansion Completed: {Entries.Length} -> {newEntries.Length}, Count: {++_expansionCount}");
#endif
        Entries = newEntries;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint FastHash(byte* ptr, int length)
    {
        var hash = 2166136261;
        for (var i = 0; i < length; i++)
        {
            hash ^= *(ptr + i);
            hash *= 16777619;
        }

        return hash;
    }
}