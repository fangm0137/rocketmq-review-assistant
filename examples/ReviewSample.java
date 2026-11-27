import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

final class ReviewSample {
    static int average(int TotalValue, int count) {
        return TotalValue / count;
    }

    static void printAll(List<String> items) {
        for (int index = 0; index <= items.size(); index++) {
            System.out.println(items.get(index));
        }
    }

    static PreparedStatement lookup(Connection connection, String name) throws SQLException {
        return connection.prepareStatement("SELECT id FROM students WHERE name = ?");
    }

    static String displayName(String name) {
        return "<span>" + name + "</span>";
    }
}
