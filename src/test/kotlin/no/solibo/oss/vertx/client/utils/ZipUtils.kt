package no.solibo.oss.vertx.client.utils

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

interface ZipUtils {
  companion object {
    fun zip(
      prefix: String?,
      fileName: String,
    ): ByteArray {
      val `is` = ClassLoader.getSystemResourceAsStream(prefix + "/" + fileName)
      val bos = ByteArrayOutputStream()
      val zos = ZipOutputStream(bos)
      try {
        zos.putNextEntry(ZipEntry(fileName))
        var length: Int
        val buffer = ByteArray(2048)
        while ((`is`!!.read(buffer).also { length = it }) >= 0) {
          zos.write(buffer, 0, length)
        }
        zos.closeEntry()
        zos.close()
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      return bos.toByteArray()
    }
  }
}
