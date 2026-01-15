package client;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.ByteBuffer;

public class Client1 {
	static void zerocopy(String ip, int port, String outputPath) {
		try {
			ByteBuffer buf = ByteBuffer.allocate(4068);
			SocketChannel channel = SocketChannel.open();
			channel.connect(new InetSocketAddress(ip, port));
			
			FileChannel outputFile = new RandomAccessFile(outputPath, "rw").getChannel();
			
			long start = System.nanoTime();
			while(channel.read(buf) > 0) {
				//System.out.println("eieiei");
				buf.flip();
				outputFile.write(buf);
				buf.clear();
			}
			
			long end = System.nanoTime();
			System.out.println("receive in " + ((end - start) / 1_000_000_000.0) + " sec with out thread");
			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	
	static void zerocopyThread(String ip, int port, String outputPath, int nThread) {
		try {
			SocketChannel client = SocketChannel.open();
			client.connect(new InetSocketAddress(ip, port));
			
			ByteBuffer buf = ByteBuffer.allocate(16);
			buf.putLong(-1L);
			buf.putLong(0L);
			buf.flip();
			while(buf.hasRemaining()) {
				client.write(buf);
			}
			
			ByteBuffer fileSizeBuf = ByteBuffer.allocate(8);
			readFully(client, fileSizeBuf);
			fileSizeBuf.flip();
			long fileSize = fileSizeBuf.getLong();
			
			RandomAccessFile output = new RandomAccessFile(outputPath, "rw");
			output.setLength(fileSize);
			
			long baseChunk = fileSize / nThread;
			long remainder = fileSize % nThread;
			System.out.println(baseChunk+ " "+ remainder);
			
			ExecutorService pool = Executors.newFixedThreadPool(Math.min(nThread, Runtime.getRuntime().availableProcessors() * 2));
			
			long start = System.nanoTime();
			
			for(int i=0; i<nThread; i++) {
				int idx = i;
				long offset = (long) idx * baseChunk;
				long length = (idx == nThread - 1) ? (baseChunk + remainder) : baseChunk;
				
				pool.submit(() -> fetchChunk(ip, port, outputPath, offset, length));
			}
			
			long end = System.nanoTime();
			System.out.println("receive in " + ((end - start)/ 1_000_000_000.0) + " sec");
			
			pool.shutdown();
			
			
		} catch (IOException e) {

			e.printStackTrace();
		}

	}
	
	static void fetchChunk(String ip, int port, String outputPath, long offset, long length) {
		try {
			SocketChannel client = SocketChannel.open();
			client.connect(new InetSocketAddress(ip, port));
			ByteBuffer buf = ByteBuffer.allocate(16);
			buf.putLong(offset);
			buf.putLong(length);
			buf.flip();
			while(buf.hasRemaining()) {
				client.write(buf);
			}
			
			FileChannel output = new RandomAccessFile(outputPath, "rw").getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
			long remaining = length;
			long written = 0;
			while(remaining > 0) {
				buffer.clear();
				int r = client.read(buffer);
				buffer.flip();
				while(buffer.hasRemaining()) {
					written += output.write(buffer, offset + written);
				}
				remaining -= r;
			}
			output.force(true);
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	static void readFully(SocketChannel c, ByteBuffer b) throws IOException {
		while(b.hasRemaining()) {
			c.read(b);
		}
	}
	
	public static void main(String[] args) {
		Client1.zerocopy("192.168.1.103", 9500, "song.mp3");
		Client1.zerocopyThread("192.168.1.103", 9502, "songt.mp3", 10);
		
	}
}
