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
        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            statement.executeUpdate("CREATE TABLE IF NOT EXISTS USERS" +
                    "(ID INT AUTO_INCREMENT PRIMARY KEY, NAME VARCHAR(50), LASTNAME VARCHAR(50), AGE TINYINT)");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() throws SQLException {
        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            statement.executeUpdate("DROP TABLE IF EXISTS USERS");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        try (Connection conn = Util.getConnection();
             PreparedStatement pS = conn.prepareStatement("INSERT INTO USERS(NAME, LASTNAME, AGE)" +
                     " VALUES (?, ?, ?)")) {
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
        try (Connection conn = Util.getConnection(); PreparedStatement pS = conn.prepareStatement("DELETE FROM USERS WHERE ID=?")) {
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
        List<User> users = new ArrayList<>();

        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement();
                ResultSet rS = statement.executeQuery("SELECT ID, NAME, LASTNAME, AGE FROM USERS")) {
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
        try (Connection conn = Util.getConnection(); Statement statement = conn.createStatement()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            statement.executeUpdate("TRUNCATE USERS");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
