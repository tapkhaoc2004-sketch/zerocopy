package test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class Server {
	static void zerocopy(String filePath, int port) {
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "r");
			FileChannel fileChannel = file.getChannel();
			System.out.println(fileChannel.size());
			ServerSocketChannel server = ServerSocketChannel.open();
			server.bind(new InetSocketAddress(port));
			
			SocketChannel socketChannel = server.accept();
			if(socketChannel != null) {
				System.out.println("connnect!!");
				long start = System.nanoTime();
				long size = fileChannel.size();
				long position = 0;
				while(position < size) {
					long transferred = fileChannel.transferTo(position, size - position, socketChannel);
					position += transferred;
				}
				long end = System.nanoTime();
				System.out.println("Sent " + ((end - start) / 1_000_000_000.0) + " sec by zerocopy");
			}
			socketChannel.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static void zerocopyThread(String filePath, int port) {
		try {
			ServerSocketChannel server = ServerSocketChannel.open();
			server.bind(new InetSocketAddress(port));
			//server.configureBlocking(false);
			
			while(true) {
				SocketChannel client = server.accept();
				
				ByteBuffer offbuf = ByteBuffer.allocate(8);
				readFully(client, offbuf);
				offbuf.flip();
				long offset = offbuf.getLong();
				
				ByteBuffer lenbuf = ByteBuffer.allocate(8);
				readFully(client, lenbuf);
				lenbuf.flip();
				long length = lenbuf.getLong();
				
				RandomAccessFile file = new RandomAccessFile(filePath, "r");
				FileChannel fileChannel = file.getChannel();
				if(offset == -1L && length == 0L) {
					ByteBuffer buf = ByteBuffer.allocate(8);
					buf.putLong(fileChannel.size());
					buf.flip();
					while(buf.hasRemaining()) {
						client.write(buf);
					}
					continue;
				}
				
				long toSend = Math.max(0, Math.min(length, fileChannel.size() - offset));
				long sent = 0;
				while(sent<toSend) {
					long n = fileChannel.transferTo(offset+sent, toSend-sent, client);
					if(n <= 0) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
					sent += n;
				}
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
		while(buffer.hasRemaining()) {
			channel.read(buffer);
		}
	}
	
	public static void main(String[] args) {
		Server.zerocopy("song.mp3", 9500);
		Server.zerocopyThread("song.mp3", 9502);
	}

}
