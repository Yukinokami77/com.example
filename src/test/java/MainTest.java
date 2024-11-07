//Тест для метода convertToTimestamp

import com.example.Main;

import org.junit.jupiter.api.Test;
import java.text.ParseException;
import java.sql.Timestamp;
import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @Test
    void testConvertToTimestamp_ValidDate() throws ParseException {
        // Проверяем корректную работу метода для правильной даты
        String dateStr = "2024-11-07";
        Timestamp expected = Timestamp.valueOf("2024-11-07 00:00:00.0");
        Timestamp actual = Main.convertToTimestamp(dateStr);
        assertEquals(expected, actual);
    }

    @Test
    void testConvertToTimestamp_NullDate() throws ParseException {
        // Проверяем обработку пустой строки
        String dateStr = null;
        assertNull(Main.convertToTimestamp(dateStr));
    }

    @Test
    void testConvertToTimestamp_EmptyDate() throws ParseException {
        // Проверяем обработку пустой строки
        String dateStr = "";
        assertNull(Main.convertToTimestamp(dateStr));
    }

    @Test
    void testConvertToTimestamp_InvalidDate() {
        // Проверяем на исключение для некорректной даты
        String dateStr = "invalid-date";
        assertThrows(ParseException.class, () -> Main.convertToTimestamp(dateStr));
    }

    @Test
    void testConvertToTimestamp_ValidLeapYearDate() throws ParseException {
        // Проверяем правильность конвертации даты для високосного года
        String dateStr = "2020-02-29";
        Timestamp expected = Timestamp.valueOf("2020-02-29 00:00:00.0");
        Timestamp actual = Main.convertToTimestamp(dateStr);
        assertEquals(expected, actual);
    }
}