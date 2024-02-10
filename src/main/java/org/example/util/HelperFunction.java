package org.example.util;

import org.gradoop.common.model.impl.id.GradoopId;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HelperFunction {
    public static long convertTimeToUnix(String timestamp) throws ParseException {
        int millisIndex = timestamp.lastIndexOf('.');
        int missingZeros = 3 - (timestamp.length() - millisIndex - 1);

        if (millisIndex == -1) {
            timestamp = timestamp + ".000";
        } else if (missingZeros > 0) {
            StringBuilder zeros = new StringBuilder();
            for (int i = 0; i < missingZeros; i++) {
                zeros.append('0');
            }
            timestamp = timestamp.substring(0, millisIndex + 1) + zeros + timestamp.substring(millisIndex + 1);
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date parsedDate = dateFormat.parse(timestamp);
        return parsedDate.getTime();
    }

    public static GradoopId createGradoopID(String type, String numberString) {
        String prefix;
        switch (type) {
            case "Person":
                prefix = "000A";
                break;
            case "Loan":
                prefix = "000B";
                break;
            case "Company":
                prefix = "000C";
                break;
            case "Account":
                prefix = "000D";
                break;
            case "Medium":
                prefix = "000E";
                break;
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }

        long number;
        try {
            number = Long.parseLong(numberString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format: " + numberString, e);
        }

        String hexRepresentation = Long.toHexString(number);
        String paddedHex = String.format("%1$" + 20 + "s", hexRepresentation).replace(' ', '0');
        return GradoopId.fromString(prefix + paddedHex);
    }
}
