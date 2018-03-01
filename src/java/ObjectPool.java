package co.momomo;

import java.util.Iterator;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

public class ObjectPool implements Runnable {

    Iterator generator;
    HashSet pool;
    ReentrantLock readLock;
    Thread generatorThread;
    
    public ObjectPool(Iterable generator) {
        this.generator = generator.iterator();
        this.pool = new HashSet();
        this.addNewItem();
        this.readLock = new ReentrantLock();
        this.generatorThread = new Thread(this);
        this.generatorThread.start();
    }

    boolean addNewItem() {
        if (this.generator.hasNext()) {
            try {
                this.pool.add(this.generator.next());
                return true;
            } catch (java.util.NoSuchElementException e) {
                return false;
            }
        } else {
            return false;
        }
    }
    

    public void run() {
        while(this.generator.hasNext()) { // fill the pool with stuff as long as there is stuff
            while(true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
            try {
                Thread.sleep(100); // wait 100ms
            } catch (InterruptedException e) {
            }
            this.readLock.lock();
            try {
                if (this.pool.isEmpty()) { // is it still empty? then put something in it
                    if (!this.addNewItem()) {
                        break;
                    }
                }
            } finally {
                this.readLock.unlock();
            }
        }
    }

    public Object take() {
        while(true) {
            this.readLock.lock();
            try {
                if (this.pool.isEmpty()) {
                    this.generatorThread.interrupt();
                    continue;
                } else {
                    Object v = this.pool.iterator().next();
                    this.pool.remove(v);
                    return v;
                }
            } finally {
                this.readLock.unlock();
            }
        }
    }

    public void put(Object v) {
        this.readLock.lock();
        try {
            this.pool.add(v);
        } finally {
            this.readLock.unlock();
        }
    }

}
