import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

class ParallelFold {
    static int N = 1_000_000_000;
    static int[] a;
    static final int CORES = Runtime.getRuntime().availableProcessors();

    interface Algo { int run() throws Exception; }
    record Result(String name, int threads, String chunk, double ms, boolean ok) {}

    static void fill() { Arrays.parallelSetAll(a, i -> (i & 1) + 1); }
    static long expected() { return (long) N + N / 2; } // N/2 непарних індексів мають значення 2

    static int sequential() {
        int len = N;
        while (len > 1) {
            int half = len >> 1;
            for (int i = 0; i < half; i++) a[i] += a[len - 1 - i];
            len -= half;
        }
        return a[0];
    }

    static volatile int curLen, curHalf;
    static volatile boolean done;

    static int managerWorkers(int P, int chunk) throws Exception {
        CyclicBarrier start = new CyclicBarrier(P + 1), end = new CyclicBarrier(P + 1);
        AtomicInteger next = new AtomicInteger();
        done = false;

        Runnable worker = () -> {
            try {
                while (true) {
                    start.await();
                    if (done) return;
                    final int len = curLen, half = curHalf;
                    final int[] arr = a;
                    int s;
                    while ((s = next.getAndAdd(chunk)) < half) {
                        int e = Math.min(s + chunk, half);
                        for (int i = s; i < e; i++) arr[i] += arr[len - 1 - i];
                    }
                    end.await();
                }
            } catch (Exception ex) { throw new RuntimeException(ex); }
        };

        Thread[] ts = new Thread[P];
        for (int t = 0; t < P; t++) { ts[t] = new Thread(worker, "worker-" + t); ts[t].setDaemon(true); ts[t].start(); }

        int len = N;
        while (len > 1) {
            int half = len >> 1;
            curLen = len; curHalf = half; next.set(0);
            start.await();
            end.await();
            len -= half;
        }
        done = true;
        start.await();
        for (Thread t : ts) t.join();
        return a[0];
    }

    static int executorSlices(int P) throws Exception {
        ExecutorService ex = Executors.newFixedThreadPool(P);
        try {
            int len = N;
            while (len > 1) {
                final int L = len, half = len >> 1;
                int slices = Math.min(half, P * 4);
                int step = (half + slices - 1) / slices;
                List<Callable<Void>> tasks = new ArrayList<>(slices);
                for (int s = 0; s < half; s += step) {
                    final int from = s, to = Math.min(s + step, half);
                    tasks.add(() -> { for (int i = from; i < to; i++) a[i] += a[L - 1 - i]; return null; });
                }
                ex.invokeAll(tasks);
                len -= half;
            }
        } finally { ex.shutdown(); }
        return a[0];
    }

    static int parallelStream() {
        int len = N;
        while (len > 1) {
            final int L = len, half = len >> 1;
            IntStream.range(0, half).parallel().forEach(i -> a[i] += a[L - 1 - i]);
            len -= half;
        }
        return a[0];
    }

    static double bench(String name, int threads, String chunk, Algo algo, int reps, List<Result> out) throws Exception {
        double best = Double.MAX_VALUE; boolean ok = true;
        for (int r = 0; r < reps; r++) {
            fill();
            long t0 = System.nanoTime();
            long sum = algo.run() & 0xFFFFFFFFL;
            double ms = (System.nanoTime() - t0) / 1e6;
            ok &= (sum == expected());
            best = Math.min(best, ms);
        }
        out.add(new Result(name, threads, chunk, best, ok));
        System.out.printf("  .. %-26s thr=%-3d chunk=%-8s %9.1f ms %s%n", name, threads, chunk, best, ok ? "" : "ПОМИЛКА");
        return best;
    }

    static void main(String[] args) throws Exception {
        if (args.length > 0) N = Integer.parseInt(args[0]);
        int reps = args.length > 1 ? Integer.parseInt(args[1]) : 2;
        System.out.printf("Java %s | N=%,d | ядер: %d | повторів: %d (береться найкращий)%n",
                System.getProperty("java.version"), N, CORES, reps);
        a = new int[N];

        List<Result> res = new ArrayList<>();
        double seq = bench("Послідовний", 1, "-", ParallelFold::sequential, reps, res);

        TreeSet<Integer> threadSet = new TreeSet<>(List.of(1, 2, 4, Math.max(1, CORES / 2), CORES, CORES * 2));
        int[] chunks = {1 << 10, 1 << 14, 1 << 18, 1 << 22};
        for (int p : threadSet)
            for (int c : chunks)
                bench("Керуючий+робочі", p, String.valueOf(c), () -> managerWorkers(p, c), reps, res);

        bench("ExecutorService", CORES, "P*4 зрізи", () -> executorSlices(CORES), reps, res);
        bench("Parallel stream", CORES, "auto", ParallelFold::parallelStream, reps, res);

        System.out.println("\n================ ЗВІТ (Java) ================");
        System.out.printf("%-26s %5s %10s %11s %9s %s%n", "Підхід", "Потоки", "Порція", "Час, мс", "Приск.", "OK");
        res.stream().sorted(Comparator.comparingDouble(Result::ms)).limit(12).forEach(r ->
                System.out.printf("%-26s %5d %10s %11.1f %8.2fx %s%n",
                        r.name(), r.threads(), r.chunk(), r.ms(), seq / r.ms(), r.ok() ? "✓" : "✗"));
        Result b = res.stream().min(Comparator.comparingDouble(Result::ms)).get();
        System.out.printf("%nНайкраща конфігурація: %s, потоків=%d, порція=%s -> %.1f мс (%.2fx до послідовного)%n",
                b.name(), b.threads(), b.chunk(), b.ms(), seq / b.ms());
        System.out.printf("Сума = %,d (очікувана %,d)%n", a[0] & 0xFFFFFFFFL, expected());
    }
}
