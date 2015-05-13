package org.apache.hadoop.util;

public class PlatformName {

    private static final String platformName = System.getProperty("os.name") + "-" + System.getProperty("os.arch") + "-" + System.getProperty("sun.arch.data.model");

    public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

    public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

    public static String getPlatformName() {
        return platformName;
    }

    public static void main(String[] args) {
        System.out.println(platformName);
    }
}