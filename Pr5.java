import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class WeatherData {
    private final String city;
    private final int temperature;
    private final int humidity;
    private final int windSpeed;

    public WeatherData(String city, int temperature, int humidity, int windSpeed) {
        this.city = city; this.temperature = temperature; this.humidity = humidity; this.windSpeed = windSpeed;
    }
    public String getCity() { return city; }
    public int getTemperature() { return temperature; }
    public int getHumidity() { return humidity; }
    public int getWindSpeed() { return windSpeed; }
    @Override
    public String toString() {
        return String.format("Місто: %-6s | T: %2d°C | H: %2d%% | W: %2d м/с",
                city, temperature, humidity, windSpeed);
    }
}

public class Pr5 {

    // пул потоків для асинхронних задач
    private static final ExecutorService EXEC = Executors.newFixedThreadPool(6);

    public static void main(String[] args) {
        System.out.println("=== START (VARIANT 5) ===\n");

        // --- Task 1: thenCompose demo ---
        performDatabaseTask()
                .exceptionally(ex -> { System.err.println("[DB_TASK] Error: " + ex.getMessage()); return null; })
                .join(); // для демонстрації, чекаємо завершення

        System.out.println("\n--- Task 2: weather comparison (thenCombine, allOf, anyOf) ---");
        compareWeatherInCities()
                .whenComplete((v, ex) -> {
                    if (ex != null) System.err.println("[MAIN] Error in compareWeather: " + ex.getMessage());
                    EXEC.shutdown();
                })
                .join();
    }

    // ---------------- Task 1 ----------------
    public static CompletableFuture<Void> performDatabaseTask() {
        return CompletableFuture.supplyAsync(() -> {
                    log("[DB] Searching user...");
                    sleep(700);
                    int id = 42;
                    log("[DB] Found user id=" + id);
                    return id;
                }, EXEC)
                .thenCompose(id -> CompletableFuture.supplyAsync(() -> {
                    log("[Service] Loading details for id=" + id);
                    sleep(500);
                    return "User{ID:" + id + ", role:ADMIN}";
                }, EXEC))
                .thenAccept(data -> log("[Result] Processed: " + data));
    }

    // ---------------- Task 2 ----------------
    public static CompletableFuture<Void> compareWeatherInCities() {
        List<String> cities = List.of("Одеса", "Київ", "Львів");

        // створюємо для кожного міста CompletableFuture<WeatherData>
        List<CompletableFuture<WeatherData>> futures = cities.stream()
                .map(Pr5::fetchFullWeatherForCity)
                .collect(Collectors.toList());

        // anyOf: хто перший
        CompletableFuture<Object> first = CompletableFuture.anyOf(futures.toArray(new CompletableFuture[0]));
        first.thenAccept(obj -> {
            if (obj instanceof WeatherData) {
                WeatherData wd = (WeatherData) obj;
                log("[anyOf] First result: " + wd);
            } else {
                log("[anyOf] First completed with unexpected type: " + obj);
            }
        });

        // allOf -> збір результатів без блокування в get()
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join) // безпечний join, бо allOf гарантує завершення
                        .collect(Collectors.toList()))
                .thenAccept(list -> {
                    System.out.println("\nReceived data:");
                    list.forEach(wd -> System.out.println("  " + wd));
                    System.out.println("\n=== CONCLUSIONS ===");
                    analyzeAndRecommend(list);
                });

        return all;
    }

    private static CompletableFuture<WeatherData> fetchFullWeatherForCity(String city) {
        CompletableFuture<Integer> tempF = CompletableFuture.supplyAsync(() -> fetchTemperature(city), EXEC);
        CompletableFuture<Integer> humF  = CompletableFuture.supplyAsync(() -> fetchHumidity(city), EXEC);
        CompletableFuture<Integer> windF = CompletableFuture.supplyAsync(() -> fetchWind(city), EXEC);

        // combine temp+hum
        CompletableFuture<int[]> tempHum = tempF.thenCombine(humF, (t, h) -> new int[]{t, h});
        // combine with wind -> WeatherData
        CompletableFuture<WeatherData> full = tempHum.thenCombine(windF,
                (th, w) -> new WeatherData(city, th[0], th[1], w));

        // логування (неблокуюче)
        full.thenAccept(wd -> log("[built] " + wd));

        // додамо обробку помилок в кожен full
        return full.exceptionally(ex -> {
            System.err.println("[fetchFullWeatherForCity] Error for " + city + ": " + ex.getMessage());
            // у разі помилки повернемо "порожній" об’єкт або можна кинути RuntimeException
            return new WeatherData(city, Integer.MIN_VALUE, 0, 0);
        });
    }

    // ----- simulated API calls -----
    private static int fetchTemperature(String city) {
        log("[API-T] " + city);
        sleep(ThreadLocalRandom.current().nextInt(200, 900));
        return ThreadLocalRandom.current().nextInt(10, 35);
    }
    private static int fetchHumidity(String city) {
        log("[API-H] " + city);
        sleep(ThreadLocalRandom.current().nextInt(200, 900));
        return ThreadLocalRandom.current().nextInt(30, 90);
    }
    private static int fetchWind(String city) {
        log("[API-W] " + city);
        sleep(ThreadLocalRandom.current().nextInt(200, 900));
        return ThreadLocalRandom.current().nextInt(0, 15);
    }

    // ----- analysis -----
    private static void analyzeAndRecommend(List<WeatherData> list) {
        WeatherData hottest = list.stream().max(Comparator.comparingInt(WeatherData::getTemperature)).orElse(null);
        WeatherData coldest = list.stream().min(Comparator.comparingInt(WeatherData::getTemperature)).orElse(null);

        if (hottest != null && hottest.getTemperature() > 25) {
            System.out.println("🏖️  НА ПЛЯЖ МОЖНА: " + hottest.getCity() + " (" + hottest.getTemperature() + "°C)");
        } else {
            System.out.println("❌ Деінде прохолодно — пляж не рекомендую.");
        }

        if (coldest != null && coldest.getTemperature() < 18) {
            System.out.println("🧥 ВДЯГАЙТЕСЬ ТЕПЛІШЕ: " + coldest.getCity() + " (" + coldest.getTemperature() + "°C)");
        } else {
            System.out.println("☀️ В інших містах тепло.");
        }
    }

    private static void sleep(int ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void log(String s) { System.out.println(Thread.currentThread().getName() + " | " + s); }
}
