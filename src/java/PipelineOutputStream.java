package co.momomo;

import java.io.OutputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipelineOutputStream extends BaseOutputStream implements Runnable {

    final OutputStream backing;
    final Thread backingThread;
    final LinkedBlockingQueue<byte[]> q;
    final AtomicBoolean isClosed;
    final ReentrantLock running;

    public PipelineOutputStream(OutputStream backing, int queueSize) {
        this.backing = backing;
        this.q = new LinkedBlockingQueue<byte[]>(queueSize);
        this.isClosed = new AtomicBoolean(false);
        this.running = new ReentrantLock();
        this.backingThread = new Thread(this);
        this.backingThread.start();
    }

    public PipelineOutputStream(OutputStream backing) {
        this(backing, 10);
    }

    public void write(byte[] data) {
        if (data == null) throw new NullPointerException();
        try {
            this.q.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        try {
            this.q.put(null);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        while (true) {
            boolean isDone = false;
            this.running.lock();
            try {
                isDone = (this.q.peek() == null);
            } finally {
                this.running.unlock();
            }
            if (isDone) break;
        }
    }

    public void close() throws IOException {
        this.isClosed.set(true);
        this.running.lock();
        try {
            this.backing.close();
        } finally {
            this.running.unlock();
        }
    }

    public void run() {
        while (!this.isClosed.get()) {
            byte[] item;
            try {
                item = this.q.take();
            } catch (InterruptedException e) {
                continue;
            }
            this.running.lock();
            try {
                if (item == null) {
                    this.backing.flush();
                } else {
                    this.backing.write(item);
                }
            } catch (Exception e) {
                System.out.println(e);
            } finally {
                this.running.unlock();
            }
        }
    }


}
