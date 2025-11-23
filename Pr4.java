import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Pr4 {

    public static void main(String[] args) {
        System.out.println("=== ПОЧАТОК ВИКОНАННЯ ЗАВДАННЯ 1 ===");
        task1();

        System.out.println("\n------------------------------------\n");

        System.out.println("=== ПОЧАТОК ВИКОНАННЯ ЗАВДАННЯ 2 ===");
        task2();

        try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
    }

    // ====================== ЗАВДАННЯ 1 ======================
    private static void task1() {

        CompletableFuture<Void> future =
                // runAsync - стартовий сигнал
                CompletableFuture.runAsync(() -> {
                            long start = System.nanoTime();
                            System.out.println("Запуск асинхронної генерації масиву...");
                            long end = System.nanoTime();
                            System.out.printf("Час runAsync: %,d нс%n", (end - start));
                        })

                        // supplyAsync - генерація масиву
                        .thenCompose(v -> CompletableFuture.supplyAsync(() -> {
                            long start = System.nanoTime();

                            int[] array = new Random().ints(10, 1, 50).toArray();

                            long end = System.nanoTime();
                            System.out.println("1. Згенеровано масив: " + Arrays.toString(array));
                            System.out.printf("Час генерації: %,d нс%n", (end - start));
                            return array;
                        }))

                        // thenApplyAsync - додавання +10
                        .thenApplyAsync(array -> {
                            long start = System.nanoTime();

                            int[] modified = Arrays.stream(array).map(x -> x + 10).toArray();

                            long end = System.nanoTime();
                            System.out.println("2. Масив (+10): " + Arrays.toString(modified));
                            System.out.printf("Час додавання: %,d нс%n", (end - start));
                            return modified;
                        })

                        // thenApplyAsync - ділення на 2
                        .thenApplyAsync(array -> {
                            long start = System.nanoTime();

                            double[] divided = Arrays.stream(array).asDoubleStream().map(x -> x / 2.0).toArray();

                            long end = System.nanoTime();
                            System.out.printf("Час ділення: %,d нс%n", (end - start));
                            return divided;
                        })

                        // thenAcceptAsync - виведення результату
                        .thenAcceptAsync(result -> {
                            long start = System.nanoTime();

                            System.out.println("3. Результат ділення (фінальний масив): " + Arrays.toString(result));

                            long end = System.nanoTime();
                            System.out.printf("Час виводу: %,d нс%n", (end - start));
                        })

                        // thenRunAsync - фінальне повідомлення
                        .thenRunAsync(() -> {
                            System.out.println("Завдання 1 повністю завершено.\n");
                        });

        future.join();
    }

    // ====================== ЗАВДАННЯ 2 ======================
    private static void task2() {

        long totalStart = System.nanoTime();

        CompletableFuture<Void> future =
                // runAsync - стартове повідомлення
                CompletableFuture.runAsync(() -> {
                            long start = System.nanoTime();
                            System.out.println("Початок обчислення послідовності...");
                            long end = System.nanoTime();
                            System.out.printf("Час runAsync: %,d нс%n", (end - start));
                        })

                        // supplyAsync - генерація послідовності
                        .thenCompose(v -> CompletableFuture.supplyAsync(() -> {
                            long start = System.nanoTime();

                            double[] seq = new double[20];
                            for (int i = 0; i < seq.length; i++) {
                                seq[i] = ThreadLocalRandom.current().nextDouble(1.0, 100.0);
                            }

                            long end = System.nanoTime();
                            System.out.println("Згенерована послідовність: " +
                                    Arrays.stream(seq)
                                            .mapToObj(x -> String.format("%.2f", x))
                                            .collect(Collectors.joining(", ", "[", "]")));
                            System.out.printf("Час генерації: %,d нс%n", (end - start));

                            return seq;
                        }))

                        // thenApplyAsync - обчислення добутку
                        .thenApplyAsync(seq -> {
                            long start = System.nanoTime();

                            double result = 1.0;
                            for (int i = 0; i < seq.length - 1; i++) {
                                result *= (seq[i + 1] - seq[i]);
                            }

                            long end = System.nanoTime();
                            System.out.printf("Час обчислення добутку: %,d нс%n", (end - start));
                            return result;
                        })

                        // thenAcceptAsync - виведення результату
                        .thenAcceptAsync(result -> {
                            long start = System.nanoTime();

                            System.out.printf("Результат обчислення: %.4f%n", result);

                            long end = System.nanoTime();
                            System.out.printf("Час виводу результату: %,d нс%n", (end - start));
                        })

                        // thenRunAsync - фінальний етап
                        .thenRunAsync(() -> {
                            long totalEnd = System.nanoTime();
                            System.out.printf("Загальний час всіх асинхронних операцій: %,d нс%n", (totalEnd - totalStart));
                        });

        future.join();
    }
}
