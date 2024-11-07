// Тест записи данных в БД

import com.example.Main;

import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class MainTest_3 {

    @Test
    void testStoreDataInDatabase_ValidData() throws Exception {
        // Мокаем соединение с базой данных
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);

        // Мокаем поведение подготовленного запроса
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        // Пример данных
        String jsonString = "{\"data\":[{\"id\":1,\"info\":{\"regNum\":\"12345\"}}]}";
        JsonNode data = new ObjectMapper().readTree(jsonString);

        // Вызов метода с моками
        Main.storeDataInDatabase(data, "2023-01-01", "2024-01-01");

        // Проверяем, что метод executeUpdate был вызван
        verify(mockStatement, times(1)).executeUpdate();
    }

    @Test
    void testStoreDataInDatabase_SqlException() throws Exception {
        // Мокаем исключение при работе с БД
        Connection mockConnection = mock(Connection.class);
        when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("Database error"));

        // Пример данных
        String jsonString = "{\"data\":[{\"id\":1,\"info\":{\"regNum\":\"12345\"}}]}";
        JsonNode data = new ObjectMapper().readTree(jsonString);

        // Проверка выбрасывания исключения
        assertThrows(SQLException.class, () -> Main.storeDataInDatabase(data, "2023-01-01", "2024-01-01"));
    }
}