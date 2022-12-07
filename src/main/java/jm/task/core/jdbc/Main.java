package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {

        UserService userService = new UserServiceImpl();

        userService.createUsersTable();

        userService.saveUser("Женя", "Козлов", (byte) 27);
        userService.saveUser("Антон", "Кадыров", (byte) 23);
        userService.saveUser("Александра", "Новикова", (byte) 21);

        System.out.println(userService.getAllUsers());

        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
