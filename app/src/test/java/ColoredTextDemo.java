import kotlin.ranges.IntRange;
import me.gegenbauer.catspy.HtmlStringRender;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.net.URI;

public class ColoredTextDemo {

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Colored Text Demo With Link");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setSize(300, 200);
            frame.setLayout(new BorderLayout());

            String raw = "01234567890123456789";

            HtmlStringRender render = new HtmlStringRender();
            render.bold(0, 5);
            render.highlight(0, 10, Color.BLUE);
            render.highlight(4, 12, Color.RED);
            render.italic(4, 12);
            JLabel label = new JLabel(render.render(raw));
            label.setFont(new Font("Arial", Font.PLAIN, 20));
            frame.add(label, BorderLayout.CENTER);

            frame.setVisible(true);
        });
    }
}