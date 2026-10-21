using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Brado.Benchmarks.FrameCodecBenchmarks).Assembly).Run(args);
