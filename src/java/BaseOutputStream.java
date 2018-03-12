package co.momomo;

import java.io.OutputStream;
import java.io.IOException;

public abstract class BaseOutputStream extends OutputStream {

    public void write(byte[] arr, int off, int len) throws IOException {
        byte[] slice = new byte[len];
        System.arraycopy(arr, off, slice, 0, len);
        this.write(slice);
    }

    public void write(int b) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte) b;
        this.write(buf);
    }

}
