package me.gegenbauer.catspy.log.datasource

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOn
import me.gegenbauer.catspy.concurrency.GIO
import me.gegenbauer.catspy.log.parse.LogParser
import okio.FileSystem
import okio.Path.Companion.toOkioPath
import okio.Path.Companion.toPath
import okio.openZip
import java.io.File

class FileLogProducer(
    private val logPath: String,
    logParser: LogParser,
    override val dispatcher: CoroutineDispatcher = Dispatchers.GIO
) : BaseLogProducer(logParser) {

    override val tempFile: File = File(logPath)

    private var flowScope: ProducerScope<*>? = null

    override fun start(): Flow<Result<LogItem>> {
        if (logPath.isBlank() || tempFile.exists().not()) {
            return emptyFlow()
        }
        return channelFlow {
            flowScope = this
            val dateRegex = """^\d{4}-\d{2}-\d{2}\s""".toRegex()
            moveToState(LogProducer.State.RUNNING)
            if (tempFile.extension.equals("zip", true)) {
                val fileSystem = FileSystem.SYSTEM
                val zipFileSystem = fileSystem.openZip(tempFile.toOkioPath())
                val files = zipFileSystem.listOrNull("/".toPath())
                if (files != null) {
                    zipFileSystem.read(files.first()) {
                        readUtf8().lineSequence().mergeNonDateStrings(dateRegex).forEach { line ->
                            suspender.checkSuspend()
                            val num = logNum.getAndIncrement()
                            send(Result.success(LogItem(num, logParser.parse(line))))
                        }
                    }
                }
            } else {
                tempFile.inputStream().bufferedReader().use { reader ->
                    reader.lineSequence().mergeNonDateStrings(dateRegex).forEach { line ->
                        suspender.checkSuspend()
                        val num = logNum.getAndIncrement()
                        send(Result.success(LogItem(num, logParser.parse(line))))
                    }
                }
            }
            invokeOnClose { moveToState(LogProducer.State.COMPLETE) }
        }.flowOn(dispatcher)
    }

    fun Sequence<String>.mergeNonDateStrings(dateRegex: Regex): Sequence<String> {
        return sequence {
            val iterator = this@mergeNonDateStrings.iterator()
            val buffer = StringBuilder()

            while (iterator.hasNext()) {
                val line = iterator.next()
                val isNonDateLine = !dateRegex.containsMatchIn(line)
                if (buffer.isEmpty() || isNonDateLine) {
                    buffer.append(if (isNonDateLine) "\n" else "").append(line)
                } else {
                    yield(buffer.toString())
                    buffer.clear()
                    buffer.append(line)
                }
            }

            if (buffer.isNotEmpty()) {
                yield(buffer.toString())
            }
        }
    }

    override fun cancel() {
        super.cancel()
        flowScope?.cancel()
    }
}