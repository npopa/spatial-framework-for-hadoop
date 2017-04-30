package com.esri.hadoop.hive;

import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;

import com.esri.core.geometry.Envelope;

public class BinUtils {
	final long numCols;
	final double extentMin;
	final double extentMax;
	final double binSize;
	
	public BinUtils(double binSize) {
		this.binSize = binSize;
		
		// absolute max number of rows/columns we can have
		long maxBinsPerAxis = (long) Math.sqrt(Long.MAX_VALUE);
		
		// a smaller binSize gives us a smaller extent width and height that
		// can be addressed by a single 64 bit long
		double size = (binSize < 1) ? maxBinsPerAxis * binSize : maxBinsPerAxis;
		
		extentMax = size/2;
		extentMin = extentMax - size;
		numCols = (long)(Math.ceil(size / binSize));
	}
	
	/**
	 * Gets bin ID from a point.
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	public long getId(double x, double y) {
		double down = (extentMax - y) / binSize;
		double over = (x - extentMin) / binSize;
		
		return ((long)down * numCols) + (long)over;
	}
	
	/**
	 * Gets the envelope for the bin ID.
	 * 
	 * @param binId
	 * @param envelope
	 */
	public void queryEnvelope(long binId, Envelope envelope) {
		long down = binId / numCols;
		long over = binId % numCols;
		
		double xmin = extentMin + (over * binSize);
		double xmax = xmin + binSize;
		double ymax = extentMax - (down * binSize);
		double ymin = ymax - binSize;
		
		envelope.setCoords(xmin, ymin, xmax, ymax);
	}
	
	/**
	 * Gets the envelope for the bin that contains the x,y coords.
	 * 
	 * @param x
	 * @param y
	 * @param envelope
	 */
	public void queryEnvelope(double x, double y, Envelope envelope) {
		double down = (extentMax - y) / binSize;
		double over = (x - extentMin) / binSize;
		
		double xmin = extentMin + (over * binSize);
		double xmax = xmin + binSize;
		double ymax = extentMax - (down * binSize);
		double ymin = ymax - binSize;
		
		envelope.setCoords(xmin, ymin, xmax, ymax);
	}
	
	/**
	 * Gets bin IDs from an envelope.
	 * 
	 * @Envelope envelope
	 * @return
	 */
	public ArrayList<LongWritable> getIds(Envelope envelope) {
		
		
		ArrayList<LongWritable> result = new ArrayList<LongWritable>();

		double maxx=envelope.getXMax();
		double maxy=envelope.getYMax();

		double minx=envelope.getXMin();
		double miny=envelope.getYMin();		
		
		double down1 = (extentMax - maxy) / binSize;
		double over1 = (maxx - extentMin) / binSize;

		double down2 = (extentMax - miny) / binSize;
		double over2 = (minx - extentMin) / binSize;
		
		double mindown = down1 > down2 ? down2 : down1;
		double maxdown = down1 > down2 ? down1 : down2;

		double minover = over1 > over2 ? over2 : over1;
		double maxover = over1 > over2 ? over1 : over2;
		
        for(long i=(long) mindown;i<=maxdown;i++){
        	for(long j=(long) minover;j<=maxover;j++){
        		result.add(new LongWritable((i * numCols) + j));
        	}
        }
		
		return result;
	}
}
