package okio

import java.io.InterruptedIOException
import java.nio.file.FileSystem as JavaNioFileSystem
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path as NioPath
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import kotlin.io.path.createDirectory
import kotlin.io.path.exists
import kotlin.io.path.inputStream
import kotlin.io.path.isSymbolicLink
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.outputStream
import kotlin.io.path.readSymbolicLink
import okio.Path.Companion.toOkioPath
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement

/**
 * A file system that wraps a `java.nio.file.FileSystem` and executes all operations in the context of the wrapped file
 * system.
 */
@IgnoreJRERequirement // Only used on platforms that support java.nio.file
internal class NioFileSystemWrappingFileSystem(javaNioFileSystem: JavaNioFileSystem) : NioSystemFileSystem() {
  // TODO(Benoit) How do deal with multiple directories?
  private val delegateRoot = javaNioFileSystem.rootDirectories.first()

  /**
   * On a `java.nio.file.FileSystem`, `java.nio.file.Path` are stateful and hold a reference to the file system they
   * got provided from. We need to [resolve][java.nio.file.Path.resolve] all okio paths before doing operations on the
   * nio file system in order for things to work properly.
   */
  private fun Path.resolve(readSymlink: Boolean = false): NioPath {
    val resolved = delegateRoot.resolve(toString())
    return if (readSymlink && resolved.isSymbolicLink()) {
      resolved.readSymbolicLink()
    } else {
      resolved
    }
  }

  override fun canonicalize(path: Path): Path {
    val canonicalFile = try {
      path.resolve().toRealPath().toOkioPath()
    } catch (e: NoSuchFileException) {
      throw FileNotFoundException("no such file: $path")
    }
    if (!canonicalFile.resolve().exists()) throw FileNotFoundException("no such file")
    return canonicalFile
  }

  override fun metadataOrNull(path: Path): FileMetadata? {
    return metadataOrNull(path.resolve())
  }

  override fun list(dir: Path): List<Path> = list(dir, throwOnFailure = true)!!

  override fun listOrNull(dir: Path): List<Path>? = list(dir, throwOnFailure = false)

  private fun list(dir: Path, throwOnFailure: Boolean): List<Path>? {
    val file = dir.resolve()
    val entries = try {
      file.listDirectoryEntries()
    } catch (e: Exception) {
      if (throwOnFailure) {
        if (!file.exists()) throw FileNotFoundException("no such file: $dir")
        throw IOException("failed to list $dir")
      } else {
        return null
      }
    }
    val result = entries.mapTo(mutableListOf()) { entry ->
      // TODO(Benoit) This whole block can surely be improved.
      val path = dir / entry.toOkioPath()
      if (dir.isRelative) {
        // All `entries` are absolute and resolving them against `dir` won't have any effect. We need to manually
        // relativize them back.
        file.relativize(path.resolve()).toOkioPath()
      } else {
        path
      }
    }
    result.sort()
    return result
  }

  override fun openReadOnly(file: Path): FileHandle {
    val channel = try {
      Files.newByteChannel(file.resolve(), StandardOpenOption.READ)
    } catch (e: NoSuchFileException) {
      throw FileNotFoundException("no such file: $file")
    }
    return JvmFileSystemFileHandle(readWrite = false, seekableByteChannel = channel)
  }

  override fun openReadWrite(file: Path, mustCreate: Boolean, mustExist: Boolean): FileHandle {
    require(!mustCreate || !mustExist) { "Cannot require mustCreate and mustExist at the same time." }
    val openOptions = mutableListOf(StandardOpenOption.READ, StandardOpenOption.WRITE)
    if (mustCreate) {
      openOptions.add(StandardOpenOption.CREATE_NEW)
    } else if (!mustExist) {
      openOptions.add(StandardOpenOption.CREATE)
    }

    val channel = try {
      Files.newByteChannel(file.resolve(), *openOptions.toTypedArray())
    } catch (e: NoSuchFileException) {
      throw FileNotFoundException("no such file: $file")
    }
    return JvmFileSystemFileHandle(readWrite = true, seekableByteChannel = channel)
  }

  override fun source(file: Path): Source {
    try {
      return file.resolve(readSymlink = true).inputStream().source()
    } catch (e: NoSuchFileException) {
      throw FileNotFoundException("no such file: $file")
    }
  }

  override fun sink(file: Path, mustCreate: Boolean): Sink {
    if (mustCreate) file.requireCreate()
    try {
      return file.resolve(readSymlink = true).outputStream().sink()
    } catch (e: NoSuchFileException) {
      throw FileNotFoundException("no such file: $file")
    }
  }

  override fun appendingSink(file: Path, mustExist: Boolean): Sink {
    if (mustExist) file.requireExist()
    return file.resolve()
      .outputStream(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      .sink()
  }

  override fun createDirectory(dir: Path, mustCreate: Boolean) {
    val alreadyExist = metadataOrNull(dir)?.isDirectory == true
    if (alreadyExist && mustCreate) {
      throw IOException("$dir already exist.")
    }

    try {
      dir.resolve().createDirectory()
    } catch (e: IOException) {
      if (alreadyExist) return
      throw IOException("failed to create directory: $dir", e)
    }
  }

  // Note that `java.nio.file.FileSystem` allows atomic moves of a file even if the target is an existing directory.
  override fun atomicMove(source: Path, target: Path) {
    try {
      Files.move(
        source.resolve(),
        target.resolve(),
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING,
      )
    } catch (e: NoSuchFileException) {
      throw FileNotFoundException(e.message)
    } catch (e: UnsupportedOperationException) {
      throw IOException("atomic move not supported")
    }
  }

  override fun delete(path: Path, mustExist: Boolean) {
    if (Thread.interrupted()) {
      // If the current thread has been interrupted.
      throw InterruptedIOException("interrupted")
    }
    val file = path.resolve()
    try {
      Files.delete(file)
    } catch (e: NoSuchFileException) {
      if (mustExist) throw FileNotFoundException("no such file: $path")
    } catch (e: IOException) {
      if (file.exists()) throw IOException("failed to delete $path")
    }
  }

  override fun createSymlink(source: Path, target: Path) {
    Files.createSymbolicLink(source.resolve(), target.resolve())
  }

  override fun toString(): String = "NioFileSystemWrappingFileSystem"
}
