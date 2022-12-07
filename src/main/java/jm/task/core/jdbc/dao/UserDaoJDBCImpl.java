package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS USERS" +
                "(ID INT AUTO_INCREMENT PRIMARY KEY, NAME VARCHAR(50), LASTNAME VARCHAR(50), AGE TINYINT)";

        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            statement.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() throws SQLException {
        String sql = "DROP TABLE IF EXISTS USERS";

        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            statement.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        String sql = "INSERT INTO USERS(NAME, LASTNAME, AGE) VALUES (?, ?, ?)";

        try (Connection conn = Util.getConnection(); PreparedStatement pS = conn.prepareStatement(sql)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            pS.setString(1, name);
            pS.setString(2, lastName);
            pS.setByte(3, age);

            pS.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) throws SQLException {
        String sql = "DELETE FROM USERS WHERE ID=?";

        try (Connection conn = Util.getConnection(); PreparedStatement pS = conn.prepareStatement(sql)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            pS.setLong(1, id);

            pS.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() throws SQLException {
        String sql = "SELECT ID, NAME, LASTNAME, AGE FROM USERS";
        List<User> users = new ArrayList<>();

        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement();
                ResultSet rS = statement.executeQuery(sql)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            while (rS.next()) {
                Long id = rS.getLong("ID");
                String name = rS.getString("NAME");
                String lastName = rS.getString("LASTNAME");
                Byte age = rS.getByte("AGE");

                User user = new User(name, lastName, age);
                user.setId(id);
                users.add(user);
            }
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return users;
    }

    public void cleanUsersTable() throws SQLException {
        String sql = "TRUNCATE USERS";

        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            statement.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
