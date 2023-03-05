package me.gegenbauer.logviewer.ui.help

import me.gegenbauer.logviewer.strings.Strings
import me.gegenbauer.logviewer.Utils
import me.gegenbauer.logviewer.ui.button.ColorButton
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.event.ActionEvent
import java.awt.event.ActionListener
import javax.swing.*
import javax.swing.plaf.basic.BasicScrollBarUI

class HelpDialog(parent: JFrame) : JDialog(parent, Strings.HELP, true), ActionListener {
    private var mHelpTextPane: JTextPane
    private var mCloseBtn : ColorButton

    init {
        mCloseBtn = ColorButton(Strings.CLOSE)
        mCloseBtn.addActionListener(this)

        mHelpTextPane = JTextPane()
        mHelpTextPane.contentType = "text/html"

        if (Strings.lang == Strings.KO) {
            mHelpTextPane.text = HelpText.textKo
        }
        else {
            mHelpTextPane.text = HelpText.textEn
        }

        mHelpTextPane.caretPosition = 0
        val scrollPane = JScrollPane(mHelpTextPane)
        val aboutPanel = JPanel()
        scrollPane.preferredSize = Dimension(850, 800)
        scrollPane.verticalScrollBar.setUI(BasicScrollBarUI())
        scrollPane.horizontalScrollBar.setUI(BasicScrollBarUI())
        aboutPanel.add(scrollPane)

        val panel = JPanel()
        panel.layout = BorderLayout()
        panel.add(aboutPanel, BorderLayout.CENTER)

        val btnPanel = JPanel()
        btnPanel.add(mCloseBtn)
        panel.add(btnPanel, BorderLayout.SOUTH)

        contentPane.add(panel)
        pack()

        Utils.installKeyStrokeEscClosing(this)
    }

    override fun actionPerformed(e: ActionEvent?) {
        if (e?.source == mCloseBtn) {
            dispose()
        }
    }
}