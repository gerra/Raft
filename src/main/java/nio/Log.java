package nio;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by root on 14.06.16.
 */
public class Log {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

    private static boolean errorEnabled = true;
    private static boolean debugEnabled = true;
    private static boolean infoEnabled = true;

    public static void disableError() {
        errorEnabled = false;
    }

    public static void disableDebug() {
        debugEnabled = false;
    }

    public static void disableInfo() {
        infoEnabled = false;
    }

    public static void d(String tag, String message) {
        if (debugEnabled) {
            System.out.println(DATE_FORMAT.format(new Date()) + ": " + tag + "/" + message);
        }
    }

    public static void e(String tag, String message) {
        if (errorEnabled) {
            System.err.println(DATE_FORMAT.format(new Date()) + ": " + tag + "/" + message);
        }
    }

    public static void i(String tag, String message) {
        if (infoEnabled) {
            System.out.println(DATE_FORMAT.format(new Date()) + ": " + tag + "/" + message);
        }
    }
}
