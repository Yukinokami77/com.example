package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.http.impl.client.CloseableHttpClient;

import java.nio.file.*;
import java.io.File;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.net.URI;
import java.sql.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.util.Scanner;

public class Main {

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "sysdba";

    private static final String API_URL = "https://budget.gov.ru/epbs/registry/ubpandnubp/data";

    private static Connection connection;
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static XmlMapper xmlMapper = new XmlMapper();  // Для преобразования в XML

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Введите начальную дату (yyyy-MM-dd): ");
        String lastUpdateFrom = scanner.nextLine();
        System.out.print("Введите конечную дату (yyyy-MM-dd): ");
        String lastUpdateTo = scanner.nextLine();

        // Запросить путь для сохранения файла
        System.out.print("Введите путь для сохранения архива (например, C:/backup/): ");
        String savePath = scanner.nextLine();
        if (!savePath.endsWith(File.separator)) {
            savePath += File.separator;
        }

        // Установите соединение с БД
        connection = DriverManager.getConnection(DATABASE_URL, USER, PASSWORD);

        // Загрузите данные
        loadData(lastUpdateFrom, lastUpdateTo, savePath);

        // Закройте соединение
        connection.close();
        scanner.close();
    }

    private static void loadData(String lastUpdateFrom, String lastUpdateTo, String savePath) throws Exception {
        // Получаем данные из API
        JsonNode data = fetchDataFromApi(lastUpdateFrom, lastUpdateTo);

        // Записываем данные в БД
        storeDataInDatabase(data, lastUpdateFrom, lastUpdateTo);

        // Архивируем предыдущие данные
        String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String zipFileName = "archive_" + currentDate + ".zip";
        String jsonFileNameInZip = "data_" + currentDate + ".json";
        String xmlFileNameInZip = "data_" + currentDate + ".xml";

        // Сохраняем JSON и XML в ZIP-архив
        archiveDataToZip(data, savePath, zipFileName, jsonFileNameInZip, xmlFileNameInZip);
    }

    private static JsonNode fetchDataFromApi(String lastUpdateFrom, String lastUpdateTo) throws IOException {
        String url = API_URL + "?lastUpdateFrom=" + lastUpdateFrom + "&lastUpdateTo=" + lastUpdateTo;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(URI.create(url));
            try (CloseableHttpResponse response = client.execute(request)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                StringBuilder result = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    result.append(line);
                }
                return objectMapper.readTree(result.toString());
            }
        }
    }
    public static Timestamp convertToTimestamp(String dateStr) throws ParseException {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null; // Если дата пустая или отсутствует, возвращаем null
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date parsedDate = sdf.parse(dateStr);
        return new Timestamp(parsedDate.getTime());
    }
    public static JsonNode convertStringToJson(String jsonString) throws Exception {
        // Преобразуем строку JSON в объект JsonNode
        return objectMapper.readTree(jsonString);
    }

    private static void saveSuccession(Connection connection, String regNum, JsonNode succession) throws SQLException, ParseException {
        String query = "INSERT INTO ubp_successions (reg_num, parent_name, parent_code, ogrn, doc_name, doc_num, doc_date, datasource) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            setNullableString(stmt,1, regNum);
            setNullableString(stmt,2, succession.get("parentName").asText());
            setNullableString(stmt,3, succession.get("parentCode").asText());
            setNullableString(stmt,4, succession.get("ogrn").asText());
            setNullableString(stmt,5, succession.get("docname").asText());
            setNullableString(stmt,6, succession.get("numberdoc").asText());
            stmt.setTimestamp(7, convertToTimestamp(succession.get("documentdate").asText()));
            setNullableString(stmt,8, succession.get("datasource").asText());

            stmt.executeUpdate();
        }
    }

    private static void saveHead(Connection connection, String regNum, JsonNode head) throws SQLException, ParseException {
        String query = "INSERT INTO ubp_heads (reg_num, fio, post, doc_name, doc_num, doc_date, head_main) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = connection.prepareStatement(query)) {

            setNullableString(stmt,1, regNum);
            setNullableString(stmt,2, head.get("fio").asText());
            setNullableString(stmt,3, head.get("post").asText());
            setNullableString(stmt,4, head.get("docName").asText());
            setNullableString(stmt,5, head.get("docNum").asText());
            stmt.setTimestamp(6, convertToTimestamp(head.get("docDate").asText()));
            stmt.setBoolean(7, head.get("headMain").asBoolean());

            stmt.executeUpdate();
        }
    }

    private static void saveFacialAccount(Connection connection, String regNum, JsonNode account) throws SQLException, ParseException {
        String query = "INSERT INTO ubp_facial_accounts (reg_num, kind_name, kind_code, account_num, create_date, close_date, " +
                "status, open_ufk_code, open_ufk_name, account_org_code, account_org_fullname, ppo_code, ppo_name, " +
                "ref_open_ufk_code, ref_srv_ufk_code) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, regNum);
            stmt.setString(2, account.get("kindName").asText());
            stmt.setString(3, account.get("kindCode").asText());
            stmt.setString(4, account.get("num").asText());
            stmt.setTimestamp(5, convertToTimestamp(account.get("createDate").asText()));
            stmt.setTimestamp(6, account.has("closeDate") ?convertToTimestamp(account.get("closeDate").asText()) : null);
            stmt.setString(7, account.get("status").asText());
            stmt.setString(8, account.get("openUfkCode").asText());
            stmt.setString(9, account.get("openUfkName").asText());
            stmt.setString(10, account.get("accountorgcode").asText());
            stmt.setString(11, account.get("accountorgfullname").asText());
            stmt.setString(12, account.get("ppocode").asText());
            stmt.setString(13, account.get("pponame").asText());
            stmt.setString(14, account.get("refopenUfkCode").asText());
            stmt.setString(15, account.get("refsrvUfkCode").asText());

            stmt.executeUpdate();
        }
    }

    // Метод для установки строки в PreparedStatement с проверкой на пустое значение
    public static void setNullableString(PreparedStatement preparedStatement, int index, String value) throws SQLException {
        if (value == null || value.trim().isEmpty()) {
            preparedStatement.setNull(index, java.sql.Types.VARCHAR);
        } else {
            preparedStatement.setString(index, value);
        }
    }


    public static void storeDataInDatabase(JsonNode data, String lastUpdateFrom, String lastUpdateTo) throws Exception, SQLException, JsonProcessingException {
        Timestamp fromTimestamp = convertToTimestamp(lastUpdateFrom);
        Timestamp toTimestamp = convertToTimestamp(lastUpdateTo);
        JsonNode dataJN = convertStringToJson(data.toString());
        // Строка запроса для вставки данных в таблицу ubp_data
        String insertQuery = "INSERT INTO ubp_data (" +
                "id, reg_num, code, division_parent_name, division_parent_code, ogrn, full_name, short_name, " +
                "inn, kpp, reg_date, okopf_name, okopf_code, okfs_name, okfs_code, post_index, city_type, city_name, " +
                "street_type, street_name, house, oktmo_name, oktmo_code, orfk_name, orfk_code, org_status, " +
                "record_num, parent_code, parent_name, date_update, exclusion_date, " +
                "date_load, json_data) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "+
                "ON CONFLICT (id) DO UPDATE SET " +
                " reg_num = EXCLUDED.reg_num, code = EXCLUDED.code, division_parent_name = EXCLUDED.division_parent_name, " +
                "division_parent_code = EXCLUDED.division_parent_code, ogrn = EXCLUDED.ogrn";


        JsonNode dataArray = data.get("data");
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER, PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {

            // Обрабатываем каждый элемент данных из "data"
            for (JsonNode dataNode : dataArray) {
                JsonNode info = dataNode.get("info");
                String idStr = dataNode.get("id").asText();
                Integer id = Integer.valueOf(idStr);
                if (info == null) continue;
                // Заполнение PreparedStatement для вставки данных
                preparedStatement.setInt(1, id);
                setNullableString(preparedStatement, 2, info.get("regNum").asText());
                setNullableString(preparedStatement, 3, info.get("code").asText());
                setNullableString(preparedStatement, 4, info.get("divisionParentName").asText());
                setNullableString(preparedStatement, 5, info.get("divisionParentCode").asText());
                setNullableString(preparedStatement, 6, info.get("ogrn").asText());
                setNullableString(preparedStatement, 7, info.get("fullName").asText());
                setNullableString(preparedStatement, 8, info.get("shortName").asText());
                setNullableString(preparedStatement, 9, info.get("inn").asText());
                setNullableString(preparedStatement, 10, info.get("kpp").asText());
                preparedStatement.setTimestamp(11, convertToTimestamp(info.get("regDate").asText()));
                setNullableString(preparedStatement, 12, info.get("okopfName").asText());
                setNullableString(preparedStatement, 13, info.get("okopfCode").asText());
                setNullableString(preparedStatement, 14, info.get("okfsName").asText());
                setNullableString(preparedStatement, 15, info.get("okfsCode").asText());
                setNullableString(preparedStatement, 16, info.get("postIndex").asText());
                setNullableString(preparedStatement, 17, info.get("cityType").asText());
                setNullableString(preparedStatement, 18, info.get("cityName").asText());
                setNullableString(preparedStatement, 19, info.get("streetType").asText());
                setNullableString(preparedStatement, 20, info.get("streetName").asText());
                setNullableString(preparedStatement, 21, info.get("house").asText());
                setNullableString(preparedStatement, 22, info.get("oktmoName").asText());
                setNullableString(preparedStatement, 23, info.get("oktmoCode").asText());
                setNullableString(preparedStatement, 24, info.get("orfkName").asText());
                setNullableString(preparedStatement, 25, info.get("orfkCode").asText());
                setNullableString(preparedStatement, 26, info.get("orgStatus").asText());
                setNullableString(preparedStatement, 27, info.get("recordNum").asText());
                setNullableString(preparedStatement, 28, info.get("parentCode").asText());
                setNullableString(preparedStatement, 29, info.get("parentName").asText());
                preparedStatement.setTimestamp(30, convertToTimestamp(info.get("dateUpdate").asText()));
                preparedStatement.setTimestamp(31, convertToTimestamp(info.get("exclusionDate").asText()));
                preparedStatement.setTimestamp(32, convertToTimestamp(info.get("loadDate").asText()));
                // Сериализуем весь объект data в строку
                String dataAsString = objectMapper.writeValueAsString(data);
                setNullableString(preparedStatement, 33, dataAsString);

                preparedStatement.executeUpdate();

                // Сохранение данных о головах (heads)
                if (data.has("heads")) {
                    for (JsonNode head : data.get("heads")) {
                        saveHead(connection, info.get("regNum").asText(), head);
                    }
                }

                // Сохранение данных о правопреемниках (successions)
                if (data.has("successions")) {
                    for (JsonNode succession : data.get("successions")) {
                        saveSuccession(connection, info.get("regNum").asText(), succession);
                    }
                }

                // Сохранение данных о лицевых счетах (facialAccounts)
                if (data.has("facialAccounts")) {
                    for (JsonNode account : data.get("facialAccounts")) {
                        saveFacialAccount(connection, info.get("regNum").asText(), account);
                    }
                }
            }
        }
    }



    public static String convertJsonToXml(String jsonString) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(jsonString);
        try {
            return xmlMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Сохранение JSON и XML в ZIP-архив
    private static void archiveDataToZip(JsonNode data, String savePath, String zipFileName, String jsonFileNameInZip, String xmlFileNameInZip) {
        try {
            // Создаем полный путь для сохранения архива
            Path outputPath = Paths.get(savePath, zipFileName);

            try (
                    FileOutputStream fileOutputStream = new FileOutputStream(outputPath.toFile());
                    ZipOutputStream zipOutputStream = new ZipOutputStream(fileOutputStream, StandardCharsets.UTF_8);
                    Writer writer = new OutputStreamWriter(zipOutputStream, StandardCharsets.UTF_8)
            ) {
                // Записываем JSON в архив
                ZipEntry jsonEntry = new ZipEntry(jsonFileNameInZip);
                zipOutputStream.putNextEntry(jsonEntry);
                writer.write(data.toString());
                writer.flush();
                zipOutputStream.closeEntry();

                // Преобразуем JSON в XML и записываем XML в архив
                String xmlData = convertJsonToXml(data.toString());
                if (xmlData != null) {
                    ZipEntry xmlEntry = new ZipEntry(xmlFileNameInZip);
                    zipOutputStream.putNextEntry(xmlEntry);
                    writer.write(xmlData);
                    writer.flush();
                    zipOutputStream.closeEntry();
                }

                System.out.println("JSON и XML успешно сохранены в архив " + outputPath);

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

  /*private static void archivePreviousData(String lastUpdateFrom, String lastUpdateTo) throws SQLException, IOException, ParseException {
  String selectQuery = "SELECT id FROM ubp_data WHERE last_update_from = ? AND last_update_to = ?";
  try (PreparedStatement statement = connection.prepareStatement(selectQuery)) {
  Timestamp fromTimestamp = convertToTimestamp(lastUpdateFrom);
  Timestamp toTimestamp = convertToTimestamp(lastUpdateTo);
  statement.setTimestamp(1, fromTimestamp);
  statement.setTimestamp(2, toTimestamp);
  try (ResultSet resultSet = statement.executeQuery()) {
  // Создаем архив
  String zipFileName = "budget_data_" + lastUpdateFrom + "_" + lastUpdateTo + ".zip";
  try (ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(Paths.get(zipFileName)))) {
  while (resultSet.next()) {
  String id = resultSet.getString("id");
  String data = resultSet.getString("data");
  ZipEntry zipEntry = new ZipEntry("data_" + id + ".json");
  zipOut.putNextEntry(zipEntry);
  zipOut.write(data.getBytes());
  zipOut.closeEntry();
  }
  }
  }
  }
  }*/
}