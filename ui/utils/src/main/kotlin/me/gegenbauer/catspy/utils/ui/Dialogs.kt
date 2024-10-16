package me.gegenbauer.catspy.utils.ui

import me.gegenbauer.catspy.platform.userHome
import me.gegenbauer.catspy.strings.STRINGS
import java.awt.Component
import java.awt.Container
import java.awt.Dimension
import java.io.File
import javax.swing.JFileChooser
import javax.swing.JFrame
import javax.swing.JOptionPane
import javax.swing.JTable
import javax.swing.SwingUtilities
import javax.swing.event.TableModelEvent
import javax.swing.event.TableModelListener

fun showOptionDialog(
    parent: Component?,
    title: String,
    message: String,
    actions: List<Pair<String, () -> Boolean>> = emptyList(),
    defaultChoice: Int = 0,
    messageType: Int = JOptionPane.PLAIN_MESSAGE
): Boolean {
    JOptionPane.showOptionDialog(
        parent,
        message,
        title,
        JOptionPane.YES_NO_OPTION,
        messageType,
        null,
        actions.map { it.first }.toTypedArray(),
        actions.getOrNull(defaultChoice)?.first
    ).takeIf { it != JOptionPane.CLOSED_OPTION }?.let {
        return actions[it].second.invoke()
    }

    return false
}

fun showWarningDialog(
    parent: Component?,
    title: String,
    message: String,
    actions: List<Pair<String, () -> Boolean>> = emptyList(),
    defaultChoice: Int = 0
): Boolean {
    return showOptionDialog(
        parent,
        title,
        message,
        actions,
        defaultChoice,
        JOptionPane.WARNING_MESSAGE,
    )
}

fun showInfoDialog(
    parent: Component?,
    title: String,
    message: String,
    defaultChoice: Int = 0
): Boolean {
    val actions = listOf(
        STRINGS.ui.ok to { true },
    )
    return showOptionDialog(
        parent,
        title,
        message,
        actions,
        defaultChoice,
        JOptionPane.INFORMATION_MESSAGE,
    )
}

fun showSelectSingleFileDialog(
    frame: JFrame,
    title: String,
    dir: String,
): List<File> {
    return chooseMultiFiles(
        frame,
        title,
        dir,
        false,
    )
}

fun showSelectMultiFilesDialog(
    frame: JFrame,
    title: String,
    dir: String,
): List<File> {
    return chooseMultiFiles(
        frame,
        title,
        dir,
        true,
    )
}

private fun chooseMultiFiles(
    frame: JFrame,
    title: String,
    dir: String,
    multiSelection: Boolean,
): List<File> {
    val chooser = getDefaultFileChooser()
    chooser.currentDirectory = chooser.fileSystemView.createFileObject(dir.ifEmpty { userHome })
    chooser.dialogTitle = title
    chooser.preferredSize = Dimension(
        (frame.size.width / 2).coerceAtLeast(600),
        (frame.size.height / 2).coerceAtLeast(300)
    )
    chooser.isMultiSelectionEnabled = multiSelection
    val details = chooser.actionMap.get("viewTypeDetails")
    details.actionPerformed(null)

    val table = findDescendantOfType(chooser, JTable::class.java) as? JTable
    table?.model?.addTableModelListener(object : TableModelListener {
        override fun tableChanged(e: TableModelEvent) {
            table.model.removeTableModelListener(this)
            SwingUtilities.invokeLater { table.rowSorter.toggleSortOrder(2) }
            SwingUtilities.invokeLater { table.rowSorter.toggleSortOrder(2) }
        }
    })
    val result = chooser.showOpenDialog(frame)
    if (result == JFileChooser.APPROVE_OPTION) {
        return chooser.selectedFiles.toList().takeIf { it.isNotEmpty() } ?: listOf(chooser.selectedFile)
    }
    return emptyList()
}

fun findDescendantOfType(root: Container, targetType: Class<*>): Component? {
    for (component in root.components) {
        if (targetType.isInstance(component)) {
            return component
        } else if (component is Container) {
            val descendant = findDescendantOfType(component, targetType)
            if (descendant != null) {
                return descendant
            }
        }
    }
    return null
}
