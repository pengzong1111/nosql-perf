package edu.indiana.d2i.cassandra.tools;

import java.io.File;

public class Constants {

    // some constants to locate randomly distributed volume zip and mets files
    public static final String ROOT_PATH = "/hathitrustmnt";
	public static final String TO_PAIRTREE_PATH = "ingester-data/full_set";
	public static final String PAIRTREE_ROOT = "pairtree_root";
	public static final char SEPERATOR = File.separatorChar;
	public static final String VOLUME_ZIP_SUFFIX = ".zip";
	public static final String METS_XML_SUFFIX = ".mets.xml";
}
