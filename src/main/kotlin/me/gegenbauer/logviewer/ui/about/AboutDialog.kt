package me.gegenbauer.logviewer.ui.about

import me.gegenbauer.logviewer.*
import me.gegenbauer.logviewer.strings.Strings
import me.gegenbauer.logviewer.ui.MainUI
import me.gegenbauer.logviewer.ui.button.ColorButton
import java.awt.BorderLayout
import java.awt.event.ActionEvent
import java.awt.event.ActionListener
import javax.swing.JDialog
import javax.swing.JFrame
import javax.swing.JLabel
import javax.swing.JPanel

class AboutDialog(parent: JFrame) :JDialog(parent, Strings.ABOUT, true), ActionListener {
    private var aboutLabel: JLabel
    private var closeBtn : ColorButton = ColorButton(Strings.CLOSE)
    private var mainUI: MainUI

    init {
        closeBtn.addActionListener(this)
        mainUI = parent as MainUI

        aboutLabel = JLabel("<html><center><h1>LogViewer $VERSION</h1><br>cdcsman@gmail.com</center></html>")

        val aboutPanel = JPanel()
        aboutPanel.add(aboutLabel)

        val panel = JPanel()
        panel.layout = BorderLayout()
        panel.add(aboutPanel, BorderLayout.CENTER)

        val btnPanel = JPanel()
        btnPanel.add(closeBtn)
        panel.add(btnPanel, BorderLayout.SOUTH)

        contentPane.add(panel)
        pack()

        Utils.installKeyStrokeEscClosing(this)
    }

    override fun actionPerformed(event: ActionEvent) {
        if (event.source == closeBtn) {
            dispose()
        }
    }
}