using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace DiskSpeed
{
	class Program
	{
		static void Main(string[] args)
		{
			if (args.Length < 1)
			{
				System.Console.WriteLine("Usage: DiskSpeed.exe fileName [blockSizeInByte]");
				return;
			}
			int blockSize = (args.Length == 2) ? int.Parse(args[1]) : 4096;
			int nSeeks = 100;
			FileStream file = File.OpenRead(args[0]);
			long size = file.Length;
			Console.WriteLine("File: " + file.Name);
			Console.WriteLine("Block size: " + blockSize + " Bytes");
			var stopwatch = new Stopwatch();
			
			// measure seek time
			stopwatch.Start();
			int b = 0;
			if(!file.CanSeek){
				Console.WriteLine("Seeking in this file is not supported!");
				return;
			}
            Random rng = new Random();
			// seek randomly inside file 100 times
			for (int i = 0; i < nSeeks; i++) {
				file.Seek((long)(rng.NextDouble() * size), SeekOrigin.Begin);
				// do stuff to avoid that instruction gets optimized away??
				b = b ^ file.ReadByte(); // otherwise seek doesn't actually move the disk's head
			}
			stopwatch.Stop();
			System.Console.WriteLine("Did 100 random seeks over the whole file in " + stopwatch.Elapsed + " -> Seek time: " + stopwatch.ElapsedMilliseconds / 100.0 + "ms " + b);

			// measure bandwidth
			byte[] block = new byte[blockSize];
			stopwatch.Restart();
			long rest = size;
			while (file.Read(block, 0, block.Length) > 0) { } // read whole file in blockSize chunks
			stopwatch.Stop();
			Console.WriteLine("Read " + size + " Bytes in " + stopwatch.Elapsed + " -> Bandwidth: " + bandwidth(size, stopwatch.ElapsedMilliseconds) + " " + block[0]);
			
			file.Close();
		}

		private static string bandwidth(double size, double time)
		{
			double bandwidth = (size / 1000.0) / time; // KB/ms = MB/s
            return string.Format("{0:0.00}", bandwidth) + " MB/s";
		}
	}
}
