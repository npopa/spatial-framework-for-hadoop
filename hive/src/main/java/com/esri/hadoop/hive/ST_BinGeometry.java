package com.esri.hadoop.hive;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
// DoubleWritable - must use hive-serde2; the other one produces struct {value:d.d}
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(name = "ST_BinGeometry",
   value = "_FUNC_(double, geometry) - a list of bins for the geometry envelope",
   extended = "Example:\n"
)

public class ST_BinGeometry extends ST_GeometryAccessor {
	private transient BinUtils bins;

	static final Log LOG = LogFactory.getLog(ST_BinGeometry.class.getName());

	public ArrayList<LongWritable> evaluate(double binSize,BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		if (bins == null) {
			bins = new BinUtils(binSize);
		} 
		
		Envelope envBound = new Envelope();
		ogcGeometry.getEsriGeometry().queryEnvelope(envBound);

		return bins.getIds(envBound);
	}
}