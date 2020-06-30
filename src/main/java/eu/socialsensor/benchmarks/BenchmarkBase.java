package eu.socialsensor.benchmarks;

import java.io.File;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;

/**
 * Base class for benchmarks.
 * 
 * @author Alexander Patrikalakis
 */
public abstract class BenchmarkBase implements Benchmark
{
    protected final BenchmarkConfiguration bench;
    protected final File outputFile;
    protected final BenchmarkType type;

    protected BenchmarkBase(BenchmarkConfiguration bench, BenchmarkType type)
    {
        this.bench = bench;
        this.outputFile = new File(bench.getResultsPath(), type.getResultsFileName());
        this.type = type;
    }

    @Override
    public final void startBenchmark()
    {
        startBenchmarkInternal();
    }

    public abstract void startBenchmarkInternal();
}
