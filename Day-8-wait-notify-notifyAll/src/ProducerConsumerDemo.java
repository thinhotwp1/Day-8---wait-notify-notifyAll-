import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;


// Lớp buffer chia sẻ
class SharedBuffer {
    private final Queue<Integer> buffer = new LinkedList<>();
    private final int capacity;
    private final Object lock = new Object(); // Đối tượng để khóa


    public SharedBuffer(int capacity) {
        this.capacity = capacity;
    }

    // Phương thức của Producer
    public void produce(int item) throws InterruptedException {
        synchronized (lock) {
            // 1. Phải dùng 'while' để kiểm tra buffer đầy
            while (buffer.size() == capacity) {
                System.out.println("Buffer đầy, Producer [" + Thread.currentThread().getName() + "] đang chờ...");
                lock.wait(); // 2. Nhả khóa và đi ngủ
            }

            // 3. Buffer không đầy, thêm item
            buffer.add(item);
            System.out.println("Producer [" + Thread.currentThread().getName() + "] đã thêm: " + item);

            // 4. Đánh thức TẤT CẢ các luồng đang chờ (có thể là Consumer)
            lock.notify();
            if(item == 0){throw new InterruptedException("Stop !");}
        }
    }


    // Phương thức của Consumer
    public void consume() throws InterruptedException {
        synchronized (lock) {
            // 1. Phải dùng 'while' để kiểm tra buffer rỗng
            while (buffer.isEmpty()) {
                System.out.println("Buffer rỗng, Consumer [" + Thread.currentThread().getName() + "] đang chờ...");
                lock.wait(); // 2. Nhả khóa và đi ngủ
            }

            // 3. Buffer không rỗng, lấy item
            int item = buffer.poll();
            System.out.println("Consumer [" + Thread.currentThread().getName() + "] đã lấy: " + item);

            // 4. Đánh thức TẤT CẢ các luồng đang chờ (có thể là Producer)
            lock.notify();
        }
    }
}


// Lớp Producer
record Producer(SharedBuffer buffer) implements Runnable {
    @Override
    public void run() {
        try {
            Random random = new Random();
            while (true) {
                int item = random.nextInt(5);
                buffer.produce(item);
                Thread.sleep(random.nextInt(1000)); // Giả lập thời gian sản xuất
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}


// Lớp Consumer
record Consumer(SharedBuffer buffer) implements Runnable {
    @Override
    public void run() {
        try {
            while (true) {
                buffer.consume();
                Thread.sleep(new Random().nextInt(1500)); // Giả lập thời gian tiêu thụ
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}


// Hàm Main
public class ProducerConsumerDemo {
    public static void main(String[] args) {
        SharedBuffer buffer = new SharedBuffer(5); // Buffer có 5 chỗ

        Thread producer1 = new Thread(new Producer(buffer), "Producer-1");
        Thread producer2 = new Thread(new Producer(buffer), "Producer-2");
        Thread consumer1 = new Thread(new Consumer(buffer), "Consumer-1");
//        Thread consumer2 = new Thread(new Consumer(buffer), "Consumer-2");

        producer1.start();
        producer2.start();
        consumer1.start();
//        consumer2.start();
    }
}
