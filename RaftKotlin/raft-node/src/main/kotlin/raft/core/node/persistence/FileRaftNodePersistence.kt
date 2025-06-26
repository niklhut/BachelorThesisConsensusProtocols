package raft.core.node.persistence

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import raft.core.node.RaftNodePersistence
import raft.core.utils.types.Snapshot
import java.io.File
import java.io.IOException

/**
 * File based implementation of node persistence.
 */
class FileRaftNodePersistence(
    override val compactionThreshold: Int
) : RaftNodePersistence {

    private val directoryPath: String
    private val mutex = Mutex()

    init {
        val currentDir = System.getProperty("user.dir")
        directoryPath = "$currentDir/raft-snapshots"

        // Ensure the directory exists
        val directory = File(directoryPath)
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw IOException("Failed to create directory: $directoryPath")
            }
        }
    }

    private fun getFileForNode(nodeId: Int): File {
        return File(directoryPath, "snapshot_$nodeId.json")
    }

    override suspend fun saveSnapshot(snapshot: Snapshot, nodeId: Int) {
        mutex.withLock {
            val file = getFileForNode(nodeId)
            val jsonString = Json.encodeToString(snapshot)

            // Write atomically by writing to a temp file first, then renaming
            val tempFile = File("${file.absolutePath}.tmp")
            try {
                tempFile.writeText(jsonString)
                if (!tempFile.renameTo(file)) {
                    throw IOException("Failed to rename temp file to ${file.name}")
                }
            } catch (e: Exception) {
                // Clean up temp file if it exists
                if (tempFile.exists()) {
                    tempFile.delete()
                }
                throw e
            }
        }
    }

    override suspend fun loadSnapshot(nodeId: Int): Snapshot? {
        mutex.withLock {
            val file = getFileForNode(nodeId)

            if (!file.exists()) {
                return null
            }

            val jsonString = file.readText()
            return Json.decodeFromString<Snapshot>(jsonString)
        }
    }

    override suspend fun deleteSnapshot(nodeId: Int) {
        mutex.withLock {
            val file = getFileForNode(nodeId)

            if (file.exists()) {
                if (!file.delete()) {
                    throw IOException("Failed to delete snapshot file: ${file.name}")
                }
            }
        }
    }
}
