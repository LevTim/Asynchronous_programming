import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class Pr1 {

    // Загальна кількість квитків на подію
    private static final int TOTAL_TICKETS = 10;

    public static void main(String[] args) {
        // Якщо передано аргумент - використаємо симульований час
        LocalTime simulatedTime = null;
        if (args.length >= 1) {
            try {
                simulatedTime = LocalTime.parse(args[0], DateTimeFormatter.ofPattern("H:mm"));
            } catch (Exception e) {
                System.out.println("Невірний формат часу для симуляції. Використовуйте HH:mm, наприклад 02:30.");
                // продовжимо без симуляції
            }
        }

        BookingManager manager = new BookingManager(TOTAL_TICKETS, simulatedTime);

        // Створимо декілька клієнтських потоків (імітація користувачів)
        List<Thread> clients = new ArrayList<>();
        for (int i = 1; i <= 7; i++) {
            // Кожен клієнт намагається забронювати від 1 до 3 квитків
            int want = 1 + new Random().nextInt(3);
            ClientRunnable client = new ClientRunnable("Клієнт-" + i, want, manager);
            Thread t = new Thread(client, "ClientThread-" + i);
            clients.add(t);
        }

        // Покажемо початковий стан
        System.out.println("Система бронювання запущена. Квитків усього: " + TOTAL_TICKETS + "\n");

        // Запустимо потоки з невеликою паузою між стартами для наочності
        for (Thread t : clients) {
            t.start();
            sleepSilently(200); // невелика затримка для читабельного виводу
        }

        // Для демонстрації: будемо інколи показувати стан потоків
        new Thread(() -> {
            try {
                for (int i = 0; i < 20; i++) {
                    printThreadStates(clients);
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                // якщо монітор буде перерваний, нічого страшного
            }
        }, "MonitorThread").start();

        // Дочекаємось завершення всіх клієнтів
        for (Thread t : clients) {
            try {
                t.join();
            } catch (InterruptedException e) {
                System.out.println("Головний потік отримав переривання під час очікування завершення клієнтів.");
            }
        }

        // Показуємо кінцеву історію бронювань у зручному вигляді
        System.out.println("\n--- Підсумок бронювань ---");
        manager.printBookingHistoryUserFriendly();

        System.out.println("Робота системи завершена.");
    }

    private static void printThreadStates(List<Thread> threads) {
        System.out.println("[Монітор станів потоків]");
        for (Thread t : threads) {
            System.out.printf("  %s : %s\n", t.getName(), t.getState().name());
        }
        System.out.println();
    }

    private static void sleepSilently(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}

/**
 * BookingManager - керує набором квитків, семафором, історією бронювань
 */
class BookingManager {
    private final List<Ticket> tickets;
    private final Semaphore ticketSemaphore; // керує кількістю одночасно доступних квитків
    private final List<BookingRecord> history = Collections.synchronizedList(new ArrayList<>());

    // Заборонений час бронювання (включно): від 00:00 до 06:00
    private static final LocalTime FORBIDDEN_FROM = LocalTime.MIDNIGHT; // 00:00
    private static final LocalTime FORBIDDEN_TO = LocalTime.of(6, 0); // 06:00

    // Якщо передано simulatedTime - буде використано він для перевірок
    private final LocalTime simulatedTime;

    public BookingManager(int totalTickets, LocalTime simulatedTime) {
        this.simulatedTime = simulatedTime;

        tickets = new ArrayList<>();
        for (int i = 1; i <= totalTickets; i++) {
            tickets.add(new Ticket(i));
        }

        // Початковий лічильник семафора = кількість вільних квитків
        ticketSemaphore = new Semaphore(totalTickets, true); // fair=true для чесної черги
    }

    /**
     * Спроба забронювати N квитків для клієнта з іменем clientName.
     * Повертає список номерів квитків, або порожній список якщо бронювання не вдалося.
     */
    public List<Integer> tryBookTickets(String clientName, int quantity) throws InterruptedException {
        // Перевіряємо заборонений інтервал часу
        LocalTime now = (simulatedTime != null) ? simulatedTime : LocalTime.now();
        if (isForbiddenTime(now)) {
            // Додаємо запис у історію як неуспішну спробу
            history.add(new BookingRecord(clientName, quantity, false, "Бронювання заборонено з 00:00 до 06:00 (система)", now));
            return Collections.emptyList();
        }

        // Спроба отримати потрібну кількість дозволів у семафора
        // Використаємо tryAcquire з таймаутом, щоб клієнт не чекав вічно
        boolean acquired = ticketSemaphore.tryAcquire(quantity, 5, java.util.concurrent.TimeUnit.SECONDS);
        if (!acquired) {
            history.add(new BookingRecord(clientName, quantity, false, "Неможливо отримати доступ до квитків (зайнято)", now));
            return Collections.emptyList();
        }

        List<Integer> booked = new ArrayList<>();
        try {
            // Синхронізовано знаходимо вільні квитки та позначаємо їх як зайняті
            synchronized (tickets) {
                for (Ticket t : tickets) {
                    if (!t.isBooked()) {
                        t.setBooked(true);
                        booked.add(t.getId());
                        if (booked.size() == quantity) break;
                    }
                }
            }

            if (booked.size() != quantity) {
                // Несподівана ситуація: семафор дав дозволи, але квитків не вистачило.
                // У такому випадку звільнимо зайняті квитки та повернемо дозволи.
                synchronized (tickets) {
                    for (Integer id : booked) {
                        tickets.get(id - 1).setBooked(false);
                    }
                }
                ticketSemaphore.release(booked.size());
                history.add(new BookingRecord(clientName, quantity, false, "Конфлікт при бронюванні, спробуйте ще раз", now));
                return Collections.emptyList();
            }

            // Успішне бронювання
            history.add(new BookingRecord(clientName, quantity, true, "Успішно", now, booked));
            return booked;

        } finally {
            // Важливо: якщо успішно заброньовано, ми НЕ повертаємо дозволи семафору.
            // Дозволи тримаємо, бо квитки зайняті. Якщо бронювання не пройшло — вони були повернені вище.
        }
    }

    private boolean isForbiddenTime(LocalTime now) {
        if (FORBIDDEN_FROM.equals(FORBIDDEN_TO)) return false; // не буває
        // Якщо інтервал проходить через опівніч
        if (FORBIDDEN_FROM.isAfter(FORBIDDEN_TO)) {
            return now.isAfter(FORBIDDEN_FROM) || now.isBefore(FORBIDDEN_TO);
        } else {
            return !now.isBefore(FORBIDDEN_FROM) && now.isBefore(FORBIDDEN_TO);
        }
    }

    public void printBookingHistoryUserFriendly() {
        System.out.println("Історія спроб бронювання (зрозумілою мовою):");
        synchronized (history) {
            if (history.isEmpty()) {
                System.out.println("  Немає записів.");
                return;
            }
            int i = 1;
            for (BookingRecord r : history) {
                System.out.println(String.format("%d) %s", i++, r.toUserString()));
            }
        }

        // Також виведемо які квитки залишилися вільними
        List<Integer> free = new ArrayList<>();
        synchronized (tickets) {
            for (Ticket t : tickets) {
                if (!t.isBooked()) free.add(t.getId());
            }
        }
        System.out.println("\nВільні квитки: " + (free.isEmpty() ? "немає" : free.toString()));
    }
}

/**
 * Ticket - простий POJO, що представляє квиток
 */
class Ticket {
    private final int id;
    private boolean booked = false;

    public Ticket(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public boolean isBooked() {
        return booked;
    }

    public void setBooked(boolean booked) {
        this.booked = booked;
    }
}

/**
 * BookingRecord - запис про спробу бронювання (успішну або неуспішну)
 */
class BookingRecord {
    private final String clientName;
    private final int requested;
    private final boolean success;
    private final String message;
    private final LocalTime time;
    private final List<Integer> bookedIds;

    public BookingRecord(String clientName, int requested, boolean success, String message, LocalTime time) {
        this(clientName, requested, success, message, time, Collections.emptyList());
    }

    public BookingRecord(String clientName, int requested, boolean success, String message, LocalTime time, List<Integer> bookedIds) {
        this.clientName = clientName;
        this.requested = requested;
        this.success = success;
        this.message = message;
        this.time = time;
        this.bookedIds = new ArrayList<>(bookedIds);
    }

    public String toUserString() {
        if (success) {
            return String.format("%s намагався забронювати %d квитків о %s — УСПІХ. Номери квитків: %s.",
                    clientName, requested, time.toString(), bookedIds.toString());
        } else {
            return String.format("%s намагався забронювати %d квитків о %s — НЕВДАЛО: %s.",
                    clientName, requested, time.toString(), message);
        }
    }
}

/**
 * ClientRunnable - кожен клієнт працює в окремому потоці та намагається забронювати квитки
 */
class ClientRunnable implements Runnable {
    private final String name;
    private final int want;
    private final BookingManager manager;

    public ClientRunnable(String name, int want, BookingManager manager) {
        this.name = name;
        this.want = want;
        this.manager = manager;
    }

    @Override
    public void run() {
        // Демонструємо стан "NEW" -> "RUNNABLE"
        Thread current = Thread.currentThread();
        safePrint(String.format("%s створено (хоче: %d квитків). Поточний стан: %s", name, want, current.getState()));

        try {
            // Імітація підготовки: потік перейде в TIMED_WAITING під час sleep
            Thread.sleep(new Random().nextInt(500));
            safePrint(name + ": починає процедуру бронювання...");

            // Спроба забронювати
            List<Integer> result = manager.tryBookTickets(name, want);

            if (result.isEmpty()) {
                safePrint(name + ": не вдалося забронювати квитки. Деталі з історії.");
            } else {
                safePrint(name + String.format(": успішно забронював квитки: %s", result.toString()));
            }

        } catch (InterruptedException e) {
            // Обробляємо переривання потоку коректно
            Thread.currentThread().interrupt(); // відновлюємо статус переривання
            safePrint(name + ": операція була перервана.");
        } catch (Exception e) {
            // Ловимо інші несподівані помилки, щоб програма не впала
            safePrint(name + ": сталася помилка: " + e.getMessage());
        } finally {
            // Стан завершення
            safePrint(name + " завершив роботу. Поточний стан: " + Thread.currentThread().getState());
        }
    }

    // Метод безпечного виводу, щоб повідомлення не перемішувались у консолі
    private void safePrint(String s) {
        synchronized (System.out) {
            System.out.println(s);
        }
    }
}
