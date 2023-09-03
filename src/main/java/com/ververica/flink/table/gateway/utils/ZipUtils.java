package com.ververica.flink.table.gateway.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**压缩工具类.
 *
 */
public class ZipUtils {

    /**压缩文件夹.
     *
     * @param sourceFolder
     * @param zipFile
     * @throws Exception
     */
    public static void zipFolder(String sourceFolder, String zipFile) throws Exception {
        FileOutputStream fos = null;
        ZipOutputStream zos = null;
        try {
            fos = new FileOutputStream(zipFile);
            zos = new ZipOutputStream(fos);

            File sourceFile = new File(sourceFolder);
            compressFolder(sourceFile, sourceFile.getName(), zos);
        } finally {
            zos.close();
            fos.close();
        }

    }

    private static void compressFolder(File sourceFile, String sourceFileName, ZipOutputStream zos) throws Exception {
        if (sourceFile.isDirectory()) {
            File[] files = sourceFile.listFiles();
            if (files.length == 0) {
                // 如果文件夹为空，需要创建一个目录条目来保留它
                ZipEntry zipEntry = new ZipEntry(sourceFileName + "/");
                zos.putNextEntry(zipEntry);
                zos.closeEntry();
            } else {
                for (File file : files) {
                    String fileName = sourceFileName + "/" + file.getName();
                    compressFolder(file, fileName, zos);
                }
            }
        } else {
            FileInputStream fis = null;
            try {
                byte[] buffer = new byte[4096];
                fis = new FileInputStream(sourceFile);
                zos.putNextEntry(new ZipEntry(sourceFileName));
                int length;
                while ((length = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, length);
                }
            } finally {
                fis.close();
                zos.closeEntry();

            }
        }
    }
}
