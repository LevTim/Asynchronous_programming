import java.util.*;
import java.util.concurrent.*;

public class Pr2 {

    private static final int NUM_THREADS = 4;


    static class PairwiseMultiplier implements Callable<int[]> {
        private final int[] array;
        private final int start;
        private final int end;

        public PairwiseMultiplier(int[] array, int start, int end) {
            this.array = array;
            this.start = start;
            this.end = end;
        }

        @Override
        public int[] call() {
            List<Integer> results = new ArrayList<>();

            for (int i = start; i < end; i += 2) {
                if (i + 1 < end) {
                    results.add(array[i] * array[i + 1]);
                }

                // Перевірка на переривання
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println("Потік " + Thread.currentThread().getName() + " перервано.");
                    break;
                }
            }

            return results.stream().mapToInt(Integer::intValue).toArray();
        }
    }

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        int min, max;

        // Ввід мінімуму
        while (true) {
            System.out.print("Введіть мінімальне число діапазону (0–100): ");
            min = sc.nextInt();
            if (min >= 0 && min <= 100) break;
            System.out.println("Помилка! Значення має бути в межах 0–100.");
        }

        // Ввід максимуму
        while (true) {
            System.out.print("Введіть максимальне число діапазону (0–100): ");
            max = sc.nextInt();
            if (max >= min && max <= 100) break;
            System.out.println("Помилка! Значення має бути в межах 0–100 і ≥ мінімального.");
        }

        long startTime = System.nanoTime();
        Random random = new Random();

        // Генерація 40–60 елементів
        int arraySize = 40 + random.nextInt(21);
        int[] array = new int[arraySize];

        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextInt(max - min + 1) + min;
        }

        System.out.println("\nЗгенерований масив (" + arraySize + " елементів):");
        System.out.println(Arrays.toString(array));

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Унікальні значення
        CopyOnWriteArraySet<Integer> uniqueProducts = new CopyOnWriteArraySet<>();
        List<Future<int[]>> futures = new ArrayList<>();

        // Розбиття масиву
        int totalPairs = arraySize / 2;
        int pairsPerThread = (int) Math.ceil((double) totalPairs / NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            int start = i * pairsPerThread * 2;
            int end = Math.min(arraySize, start + pairsPerThread * 2);

            if (start >= arraySize) break;

            System.out.println("Старт потоку для індексів: " + start + " - " + (end - 1));

            PairwiseMultiplier task = new PairwiseMultiplier(array, start, end);
            Future<int[]> future = executor.submit(task);
            futures.add(future);
        }

        List<Integer> allProducts = new ArrayList<>();

        // Отримання результатів
        for (Future<int[]> future : futures) {
            try {
                if (future.isCancelled()) {
                    System.out.println("Завдання скасовано.");
                    continue;
                }

                int[] partResults = future.get();  // Блокуюче отримання

                if (future.isDone()) {
                    for (int val : partResults) {
                        allProducts.add(val);
                        uniqueProducts.add(val);
                    }
                }

            } catch (Exception e) {
                System.out.println("Помилка під час виконання потоку: " + e.getMessage());
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        long endTime = System.nanoTime();
        long ms = (endTime - startTime) / 1_000_000;

        // Вивід результатів
        System.out.println("\n--- РЕЗУЛЬТАТИ ---");
        System.out.println("Усі попарні добутки:");
        System.out.println(allProducts);

        System.out.println("\nУнікальні значення (CopyOnWriteArraySet):");
        System.out.println(uniqueProducts);

        System.out.println("\nЧас виконання програми: " + ms + " мс");
    }
}
