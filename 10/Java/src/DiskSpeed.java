import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskSpeed {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.err.println("Usage: java DiskSpeed fileName [blockSizeInByte]");
			return;
		}
		// block size is probably 4K on most systems
		int blockSize = (args.length == 2) ? Integer.valueOf(args[1]) : 4096;
		int nSeeks = 100;
		// open RandomAccessFile
		File file = new File(args[0]);
		long size = file.length();
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		System.out.println("File: " + file.getAbsolutePath());
		System.out.println("Block size: " + blockSize + " Bytes");
		long start, time;
		
		// measure seek time
		start = System.nanoTime();
		int b = 0;
		// seek randomly inside file 100 times
		for (int i = 0; i < nSeeks; i++) {
			raf.seek((long) (Math.random() * size));
			// do stuff to avoid that instruction gets optimized away??
			b = b ^ raf.read(); // otherwise seek doesn't actually move the disk's head
		}
		time = System.nanoTime() - start;
		System.out.println("Did 100 random seeks over the whole file in " + time(time) + " -> Seek time: " + time(time / 100));
		raf.close();
		
		// close and reopen to avoid cache??
		raf = new RandomAccessFile(file, "r");
		
		// measure bandwidth
		byte[] block = new byte[blockSize];
		start = System.nanoTime();
		while (raf.read(block) != -1) {} // read whole file in blockSize chunks
		time = System.nanoTime() - start;
		System.out.println("Read " + size + " Bytes in " + time(time) + " -> Bandwidth: " + bandwidth(size, time));
		raf.close();
		
		System.out.println();
	}

	private static String time(long time) {
		int i = 0;
		double t = time;
		String[] units = new String[] { "ns", "µs", "ms", "s" };
		while (t > 1000 && i < units.length) {
			t /= 1000.;
			i++;
		}
		return String.format("%.2f", t) + " " + units[i];
	}

	private static String bandwidth(long size, long time) {
		double bandwidth = size / (time / 1000.);
		return String.format("%.2f", bandwidth) + " MB/s";
	}
}
