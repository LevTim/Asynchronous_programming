import java.awt.Desktop;
import java.awt.Point;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;

public class Pr3 {

    private static final Scanner scanner = new Scanner(System.in);
    private static final Random random = new Random();

    public static void main(String[] args) {
        while (true) {
            System.out.println("\n--- ГОЛОВНЕ МЕНЮ ---");
            System.out.println("1. Завдання 1 (Пошук в матриці: Work Stealing vs Work Dealing)");
            System.out.println("2. Завдання 2 (Пошук зображень: Work Stealing)");
            System.out.println("0. Вихід");
            System.out.print("Виберіть опцію: ");

            int choice = getIntInput(0, 2);

            switch (choice) {
                case 1:
                    runTask1();
                    break;
                case 2:
                    runTask2();
                    break;
                case 0:
                    System.out.println("Завершення роботи.");
                    scanner.close();
                    return;
            }
        }
    }

    // ЗАВДАННЯ 1: ПОШУК В МАТРИЦІ

    private static void runTask1() {
        System.out.println("\n--- Завдання 1: Пошук в матриці (value == i + j) ---");

        // Отримання вхідних даних від користувача з перевіркою
        int rows = getIntInput("Введіть кількість рядків (наприклад, 10000): ", 1, Integer.MAX_VALUE);
        int cols = getIntInput("Введіть кількість стовпців (наприклад, 10000): ", 1, Integer.MAX_VALUE);
        int min = getIntInput("Введіть мінімальне значення для генерації: ", Integer.MIN_VALUE, Integer.MAX_VALUE);
        int max;
        while (true) {
            max = getIntInput("Введіть максимальне значення для генерації: ", Integer.MIN_VALUE, Integer.MAX_VALUE);
            if (max >= min) break;
            System.out.println("Помилка: Максимальне значення має бути більше або рівне мінімальному.");
        }

        // Генерація матриці
        System.out.println("Генерація матриці " + rows + "x" + cols + "...");
        int[][] matrix = generateMatrix(rows, cols, min, max);
        System.out.println("Матрицю згенеровано.");

        // Друк матриці, якщо вона не надто велика
        if (rows <= 20 && cols <= 20) {
            printMatrix(matrix);
        }

        // Виконання: Work Stealing (ForkJoinPool)
        System.out.println("\n--- 1. Виконання через Work Stealing (ForkJoinPool) ---");
        long startTimeStealing = System.nanoTime();
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        MatrixSearchTask taskStealing = new MatrixSearchTask(matrix, 0, rows);
        Point resultStealing = forkJoinPool.invoke(taskStealing);
        long durationStealing = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeStealing);
        forkJoinPool.shutdown();

        printResult(matrix, resultStealing, durationStealing);

        // Виконання: Work Dealing (ExecutorService)
        System.out.println("\n--- 2. Виконання через Work Dealing (ExecutorService) ---");
        long startTimeDealing = System.nanoTime();
        Point resultDealing = findWithWorkDealing(matrix, rows);
        long durationDealing = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeDealing);

        printResult(matrix, resultDealing, durationDealing);

        // Виведення результатів
        System.out.println("\n--- Порівняння ---");
        System.out.println("Work Stealing (ForkJoinPool): " + durationStealing + " мс");
        System.out.println("Work Dealing (ExecutorService): " + durationDealing + " мс");
    }

    /**
     * Клас для Work Stealing (ForkJoinPool)
     * Використовує RecursiveTask для рекурсивного поділу завдання.
     */
    static class MatrixSearchTask extends RecursiveTask<Point> {
        private final int[][] matrix;
        private final int startRow;
        private final int endRow;
        private final int cols;
        // Поріг, при якому завдання більше не ділиться, а виконується послідовно
        private static final int THRESHOLD = 100; // Можна налаштувати

        MatrixSearchTask(int[][] matrix, int startRow, int endRow) {
            this.matrix = matrix;
            this.startRow = startRow;
            this.endRow = endRow;
            this.cols = matrix[0].length;
        }

        @Override
        protected Point compute() {
            // Якщо діапазон рядків достатньо малий, виконуємо пошук послідовно
            if ((endRow - startRow) <= THRESHOLD) {
                for (int i = startRow; i < endRow; i++) {
                    for (int j = 0; j < cols; j++) {
                        if (matrix[i][j] == i + j) {
                            return new Point(i, j); // Знайдено!
                        }
                    }
                }
                return null; // В цьому блоці не знайдено
            } else {
                // Ділимо завдання навпіл (divide-and-conquer)
                int midRow = (startRow + endRow) / 2;
                MatrixSearchTask leftTask = new MatrixSearchTask(matrix, startRow, midRow);
                MatrixSearchTask rightTask = new MatrixSearchTask(matrix, midRow, endRow);

                // Запускаємо ліву частину асинхронно (це "краде" інший потік)
                leftTask.fork();
                // Виконуємо праву частину в поточному потоці
                Point rightResult = rightTask.compute();
                // Очікуємо результат лівої частини
                Point leftResult = leftTask.join();

                // Повертаємо перший знайдений результат
                if (leftResult != null) {
                    return leftResult;
                }
                return rightResult; // Може бути null, якщо нічого не знайдено
            }
        }
    }

    /**
     * Метод для Work Dealing (ExecutorService)
     * Ми вручну "роздаємо" завдання пулу потоків.
     */
    private static Point findWithWorkDealing(int[][] matrix, int rows) {
        int cores = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(cores);
        // CompletionService повертає результати в міру їх завершення, що дозволяє нам отримати *перший* знайдений результат
        CompletionService<Point> completionService = new ExecutorCompletionService<>(executor);

        // Визначаємо розмір "шматка" роботи для кожного потоку
        int chunkSize = (int) Math.ceil((double) rows / cores);
        int taskCount = 0;

        for (int i = 0; i < cores; i++) {
            int startRow = i * chunkSize;
            int endRow = Math.min((i + 1) * chunkSize, rows);

            if (startRow >= endRow) continue; // Не створюємо пустих завдань

            // Створюємо Callable
            Callable<Point> task = new MatrixSearchCallable(matrix, startRow, endRow);
            completionService.submit(task);
            taskCount++;
        }

        Point foundResult = null;
        try {
            // Перевіряємо результати в міру їх надходження
            for (int i = 0; i < taskCount; i++) {
                Future<Point> future = completionService.take(); // Блокує, поки 1 завдання не завершиться
                Point result = future.get();
                if (result != null && foundResult == null) {
                    // Знайшли перший!
                    foundResult = result;
                    // Ми можемо зупинити інші, щойно знайшли
                    executor.shutdownNow();
                    break;
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            System.err.println("Помилка під час Work Dealing: " + e.getMessage());
        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
        return foundResult;
    }

    /**
     * Клас Callable для Work Dealing.
     * Просто виконує послідовний пошук у своєму діапазоні.
     */
    static class MatrixSearchCallable implements Callable<Point> {
        private final int[][] matrix;
        private final int startRow;
        private final int endRow;
        private final int cols;

        MatrixSearchCallable(int[][] matrix, int startRow, int endRow) {
            this.matrix = matrix;
            this.startRow = startRow;
            this.endRow = endRow;
            this.cols = matrix[0].length;
        }

        @Override
        public Point call() throws Exception {
            for (int i = startRow; i < endRow; i++) {
                // Дозволяємо переривання задачі, якщо результат вже знайшли в іншому потоці
                if (Thread.currentThread().isInterrupted()) {
                    return null;
                }
                for (int j = 0; j < cols; j++) {
                    if (matrix[i][j] == i + j) {
                        return new Point(i, j); // Знайдено!
                    }
                }
            }
            return null; // Не знайдено
        }
    }

    //  Допоміжні методи для Завдання 1

    private static int[][] generateMatrix(int rows, int cols, int min, int max) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                // +1 до max, тому що nextInt() не включає верхню межу
                matrix[i][j] = random.nextInt((max - min) + 1) + min;
            }
        }
        return matrix;
    }

    private static void printMatrix(int[][] matrix) {
        System.out.println("Згенерована матриця:");
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%5d ", val);
            }
            System.out.println();
        }
    }

    private static void printResult(int[][] matrix, Point result, long duration) {
        if (result != null) {
            System.out.println("✅ Результат знайдено: Елемент " +
                    matrix[result.x][result.y] + " на позиції [" + result.x + ", " + result.y + "]");
        } else {
            System.out.println("❌ Результат не знайдено (відповідно до умови).");
        }
        System.out.println("⏱️ Час виконання: " + duration + " мс.");
    }

    // ЗАВДАННЯ 2: ПОШУК ЗОБРАЖЕНЬ

    private static void runTask2() {
        System.out.println("\n--- Завдання 2: Пошук зображень у директорії ---");
        System.out.println("Ми будемо використовувати підхід 'Work Stealing' (ForkJoinPool).");
        System.out.println("Обґрунтування: Пошук у файловій системі незбалансований, " +
                "тому Work Stealing є найбільш ефективним.");

        // Отримання вхідних даних
        File directory;
        while (true) {
            System.out.print("Введіть повний шлях до директорії (наприклад, C:\\Users\\Public\\Pictures): ");
            String path = scanner.nextLine();
            directory = new File(path);
            if (directory.exists() && directory.isDirectory()) {
                break;
            }
            System.out.println("Помилка: Шлях не існує або це не директорія. Спробуйте ще раз.");
        }

        // Виконання
        System.out.println("Розпочинаю пошук...");
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        ImageSearchTask task = new ImageSearchTask(directory);
        ImageSearchResult result = forkJoinPool.invoke(task);
        forkJoinPool.shutdown();

        // Виведення результатів
        System.out.println("\n--- Результати Пошуку ---");
        System.out.println("✅ Загальна кількість знайдених зображень: " + result.getCount());

        if (result.getLastFoundFile() != null) {
            System.out.println("Останній знайдений файл: " + result.getLastFoundFile().getAbsolutePath());

            // Спроба відкрити останній файл
            try {
                if (Desktop.isDesktopSupported()) {
                    System.out.println("Намагаюся відкрити файл...");
                    Desktop.getDesktop().open(result.getLastFoundFile());
                } else {
                    System.out.println("Відкриття файлів не підтримується на цій системі.");
                }
            } catch (IOException e) {
                System.err.println("Помилка при відкритті файлу: " + e.getMessage());
            } catch (UnsupportedOperationException e) {
                System.err.println("Операція 'open' не підтримується на цій системі.");
            }
        } else {
            System.out.println("❌ Зображень не знайдено.");
        }
    }

    /**
     * Клас для зберігання проміжних результатів пошуку.
     */
    static class ImageSearchResult {
        private long count;
        private File lastFoundFile;

        ImageSearchResult(long count, File lastFoundFile) {
            this.count = count;
            this.lastFoundFile = lastFoundFile;
        }

        public long getCount() { return count; }
        public File getLastFoundFile() { return lastFoundFile; }

        /**
         * Об'єднує цей результат з іншим.
         */
        public void merge(ImageSearchResult other) {
            this.count += other.count;
            // "Останній" файл - це просто останній, який ми зустріли.
            // При об'єднанні беремо "останній" з підзадачі.
            if (other.lastFoundFile != null) {
                this.lastFoundFile = other.lastFoundFile;
            }
        }
    }

    /**
     * Клас RecursiveTask для пошуку файлів (Work Stealing).
     */
    static class ImageSearchTask extends RecursiveTask<ImageSearchResult> {
        private final File directory;
        private static final Set<String> IMAGE_EXTENSIONS =
                new HashSet<>(Set.of("jpg", "jpeg", "png", "gif", "bmp", "webp"));

        ImageSearchTask(File directory) {
            this.directory = directory;
        }

        @Override
        protected ImageSearchResult compute() {
            List<ImageSearchTask> subTasks = new ArrayList<>();
            long localCount = 0;
            File localLastFile = null;

            File[] files = directory.listFiles();
            if (files == null) {
                // Помилка доступу або не директорія
                return new ImageSearchResult(0, null);
            }

            // Обробляємо файли та створюємо підзадачі для директорій
            for (File file : files) {
                if (file.isDirectory()) {
                    // Це директорія - створюємо і "форкаємо" підзадачу
                    ImageSearchTask subTask = new ImageSearchTask(file);
                    subTask.fork(); // Асинхронний запуск
                    subTasks.add(subTask);
                } else {
                    // Це файл - перевіряємо розширення
                    String ext = getFileExtension(file.getName());
                    if (IMAGE_EXTENSIONS.contains(ext.toLowerCase())) {
                        localCount++;
                        localLastFile = file;
                    }
                }
            }

            // Створюємо результат для поточної директорії
            ImageSearchResult result = new ImageSearchResult(localCount, localLastFile);

            // 3. Збираємо результати з усіх підзадач
            for (ImageSearchTask subTask : subTasks) {
                ImageSearchResult subResult = subTask.join(); // Очікуємо завершення
                result.merge(subResult); // Об'єднуємо результати
            }

            return result;
        }

        private String getFileExtension(String fileName) {
            int lastDotIndex = fileName.lastIndexOf('.');
            if (lastDotIndex > 0 && lastDotIndex < fileName.length() - 1) {
                return fileName.substring(lastDotIndex + 1);
            }
            return "";
        }
    }

    // ЗАГАЛЬНІ ДОПОМІЖНІ МЕТОДИ

    /**
     * Допоміжний метод для отримання коректного цілого числа від користувача.
     */
    private static int getIntInput(String prompt, int min, int max) {
        int input;
        while (true) {
            try {
                System.out.print(prompt);
                input = scanner.nextInt();
                if (input >= min && input <= max) {
                    break;
                } else {
                    System.out.println("Помилка: Введіть число в діапазоні [" + min + ", " + max + "].");
                }
            } catch (InputMismatchException e) {
                System.out.println("Помилка: Введіть коректне ціле число.");
                scanner.next(); // Очистити буфер сканера
            }
        }
        scanner.nextLine(); // Очистити буфер (для наступного nextLine())
        return input;
    }

    private static int getIntInput(int min, int max) {
        return getIntInput("Введіть число: ", min, max);
    }
}