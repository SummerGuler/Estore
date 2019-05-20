/**
 * Summer Guler
 * It's a Java app to access the Estore database to display listed queries.
 * */

import java.sql.*;
import java.io.*;
import java.util.TimeZone;

public class Estore
{
    public static void main (String args[]) throws Exception, IOException, SQLException
    {
        // Load the JDBC driver
        try
        {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Could not load the driver");
        }
        // Connect to the estore database
        System.out.print("\nEnter your MySQL password: ");
        String password = readLine();

        String myUrl = "jdbc:mysql://localhost:3306/estore?serverTimezone=" + TimeZone.getDefault().getID();
        Connection conn = DriverManager.getConnection(myUrl,"root", password);

        System.out.println("Database connected");

        boolean done = false;
        printMenu();

        do
        {
            System.out.println();
            System.out.print("Type in your option (M for Menu): ");
            System.out.flush();
            String ch = readLine().toUpperCase();
            System.out.println();
            switch (ch)
            {
                case "1": categoryNames(conn);
                    break;
                case "2": productNamesAndPrices(conn);
                    break;
                case "3": customerOrderDetails(conn);
                    break;
                case "4": orderDates(conn);
                    break;
                case "5": ordersOver1(conn);
                    break;
                case "6": customersSpentOver300USD(conn);
                    break;
                case "7": firstThreeOrders(conn);
                    break;
                case "8": customersBuyFromVendorsH(conn);
                    break;
                case "9": productsSold2orMore(conn);
                    break;
                case "10": greaterThanOrderId7Amount(conn);
                    break;
                case "11": updateCustomerAddress(conn);
                    break;
                case "12": addAnewProduct(conn);
                    break;
                case "13": deleteAproduct(conn);
                    break;
                case "M": printMenu();
                    break;
                case "Q": done = true;
                    break;
                default: System.out.println(" Not a valid option ");
            } // switch
        } while(!done);
        conn.close();
    } // main


    private static ResultSet queryExecution(Statement stmt, String query) {
        /**
         * Receives stmt and query to return the results */

        try
        {
            ResultSet rset = stmt.executeQuery(query);
            return rset;
        }
        catch (SQLException e)
        {
            System.out.println("Could not execute query ");
            while (e != null)
            {
                System.out.println("Message: " + e.getMessage());
                e = e.getNextException();
            }
        }
        return null;
    }


    private static void printResults(ResultSet rset) {
        /**
         * Prints produced query results */

        try
        {
            rset.last();
            int rowCount = rset.getRow();
            rset.beforeFirst();

            ResultSetMetaData md = rset.getMetaData();
            int colCount = md.getColumnCount();

            for (int i = 0; i < rowCount && rset.next(); i++)
            {
                for (int j = 1; j <= colCount; j++)
                {
                    System.out.printf("| %-30s ", rset.getString(j));
                    if (j == colCount)
                        System.out.println("| ");
                }
            }
        }
        catch (SQLException e)
        {
            while (e != null)
            {
                System.out.println("Message: " + e.getMessage());
                e = e.getNextException();
            }
        }
    }


    // QUERY 1
    private static void categoryNames(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays category names
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT DISTINCT c.category_name " +
                "FROM category c " +
                "WHERE c.category_id IN (SELECT p.category_id " +
                                        "FROM product p " +
                                        "WHERE c.category_id = p.category_id) " +
                "ORDER BY category_name ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nCategory Names");
        System.out.println("----------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 2
    private static void productNamesAndPrices(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays product names and prices
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT p.product_name, p.list_price " +
                "FROM product p " +
                "WHERE p.list_price > (SELECT AVG(p.list_price) " +
                                    "FROM product p) " +
                "ORDER BY p.list_price DESC";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nProduct Names and Prices");
        System.out.println("--------------------------");

        printResults(rset);

        stmt.close();
    }


    // QUERY 3
    private static void customerOrderDetails(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays customer order details
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT x.* " +
                "FROM (SELECT c.email_address, oi.order_id, " +
                "SUM((oi.item_price * oi.quantity) - oi.discount_amount) " +
                    "AS order_total " +
                    "FROM customer c " +
                    "JOIN orders od " +
                    "ON(c.customer_id = od.customer_id) " +
                    "JOIN orderitems oi " +
                    "ON(od.order_id = oi.order_id) " +
                    "GROUP BY(oi.order_id) " +
                    "ORDER BY(c.email_address)) x";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nCustomer Email, Order Id, and Total Amount");
        System.out.println("--------------------------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 4
    private static void orderDates(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays order dates
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT x.* " +
                "FROM (SELECT c.email_address, " +
                "MIN(od.order_id) AS order_id, " +
                "MIN(od.order_date) AS order_date " +
                "FROM customer c " +
                "JOIN orders od " +
                "ON (c.customer_id = od.customer_id) " +
                "GROUP BY (c.email_address) " +
                "ORDER BY (c.email_address)) x ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nCustomer Email Adress, Order Id, and Order Date");
        System.out.println("-------------------------------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 5
    private static void ordersOver1(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays orders with 1 or more items
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT  p.product_id, p.product_name, p.list_price " +
                "FROM product p " +
                "GROUP BY (p.product_id) " +
                "HAVING COUNT((SELECT oi.product_id " +
                            "FROM orderitems oi " +
                            "LIMIT 1) > 1) ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nProduct Id, Product Name, and List Price");
        System.out.println("------------------------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 6
    private static void customersSpentOver300USD(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays customers who spent over $300
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT c.last_name, c.first_name " +
                "FROM customer c " +
                "WHERE c.customer_id IN (SELECT od.customer_id " +
                                        "FROM orders od " +
                                        "JOIN orderitems oi " +
                                        "ON (od.order_id = oi.order_id) " +
                                        "WHERE (oi.item_price > 300)) " +
                "ORDER BY c.first_name ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nCustomers Who Spent More Than $300");
        System.out.println("------------------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 7
    private static void firstThreeOrders(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays first 3 orders
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT c.last_name, c.first_name, c.email_address " +
                "FROM customer c " +
                "WHERE c.customer_id IN (SELECT od.customer_id " +
                                        "FROM orders od " +
                                        "WHERE od.order_id IN (1, 2, 3)) " +
                "ORDER BY c.last_name ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nFirst 3 Orders");
        System.out.println("----------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 8
    private static void customersBuyFromVendorsH(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays customers who buy from the vendor name start with the letter 'H'
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT c.last_name, c.first_name, v.company_name " +
                "FROM customer c, vendor v " +
                "WHERE c.customer_id IN (SELECT od.customer_id " +
                                        "FROM orders od " +
                                        "JOIN orderitems oi ON (od.order_id = oi.order_id) " +
                                        "JOIN product p ON (p.product_id = oi.product_id) " +
                                        "JOIN vendor v ON (v.vendor_id = p.vendor_id) " +
                                        "WHERE v.company_name LIKE 'H%') " +
                "AND v.company_name LIKE 'H%' " +
                "ORDER BY c.last_name ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nCustomers Who Buy From Vendors H%");
        System.out.println("-----------------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 9
    private static void productsSold2orMore(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays products that sold 2 or more
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT p.product_id, p.product_name, p.list_price " +
                "FROM product p " +
                "WHERE p.product_id IN (SELECT oi.product_id " +
                                        "FROM orderitems oi " +
                                        "GROUP BY (oi.product_id) " +
                                        "HAVING COUNT(oi.quantity) >= 2) " +
                "ORDER BY p.product_id ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nProducts Sold 2 or More");
        System.out.println("-------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 10
    private static void greaterThanOrderId7Amount(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays order ids that have greater total amount than order id 7
         * */

        Statement stmt = conn.createStatement();
        String query = "SELECT od.order_id " +
                "FROM orders od " +
                "JOIN orderitems oi " +
                "ON (od.order_id = oi.order_id) " +
                "GROUP BY oi.order_id " +
                "HAVING SUM(oi.item_price - oi.discount_amount) > (SELECT SUM(item_price - discount_amount) " +
                                                                    "FROM orderitems " +
                                                                    "WHERE order_id = 7) ";

        ResultSet rset = queryExecution(stmt, query);

        System.out.println("\nOrders with Greater Total Than Order Id 7 ");
        System.out.println("--------------------------------------------");

        printResults(rset);

        stmt.close();

    }


    // QUERY 11
    private static void updateCustomerAddress(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays and updates the Customer Address
         * */

        System.out.print("Enter the Customer Id: ");
        int customer_id = Integer.parseInt(readLine());
        System.out.print("Please enter the Address Id: ");
        int address_id = Integer.parseInt(readLine());
        System.out.print("Enter the new Address Line 1 info: ");
        String newLine1 = readLine();
        System.out.print("Enter the new Address Line 2 info: ");
        String newLine2 = readLine();
        System.out.print("Enter the new City info: ");
        String newCity = readLine();
        System.out.print("Enter the new State info: ");
        String newState = readLine();
        System.out.print("Enter the new Zip Code: ");
        int newZipCode = Integer.parseInt(readLine());

        Statement stmt = conn.createStatement();
        String query = "SELECT c.last_name, c.first_name, a.line1, a.line2, a.city, a.state, a.zip_code " +
                "FROM customer c " +
                "JOIN address a " +
                "ON (c.customer_id = a.customer_id) " +
                "WHERE c.customer_id = " + customer_id;

        ResultSet rset1 = queryExecution(stmt, query);

        System.out.println("\nAddress info Before Your Update ");
        System.out.println("----------------------------------");

        printResults(rset1);

        PreparedStatement update = conn.prepareStatement
                ("UPDATE address a SET a.line1 = ?, a.line2 = ?, a.city = ?, a.state = ?, " +
                        "a.zip_code = ? WHERE a.customer_id = ? AND a.address_id = ?");

        update.setString(1, newLine1);
        update.setString(2, newLine2);
        update.setString(3, newCity);
        update.setString(4, newState);
        update.setInt(5, newZipCode);
        update.setInt(6, customer_id);
        update.setInt(7, address_id);

        update.executeLargeUpdate();

        ResultSet rset2 = queryExecution(stmt, query);

        System.out.println("\nAddress info After Your Update ");
        System.out.println("---------------------------------");

        printResults(rset2);

        stmt.close();

    }


    // QUERY 12
    private static void addAnewProduct(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays and adds a new product info
         * */

        System.out.print("Enter the new Product Id: ");
        int product_id = Integer.parseInt(readLine());
        System.out.print("Please enter its Category Id: ");
        int category_id = Integer.parseInt(readLine());
        System.out.print("Enter its Product Code: ");
        String product_code = readLine();
        System.out.print("Enter its Product Name: ");
        String product_name = readLine();
        System.out.print("Enter its Description: ");
        String description = readLine();
        System.out.print("Enter its List Price: ");
        float list_price = Float.parseFloat(readLine());
        System.out.print("Enter its Discount Percent: ");
        float discount_percent = Float.parseFloat(readLine());
        System.out.print("Enter the Date Added: ");
        String date_added = readLine();
        System.out.print("Enter its Vendor Id: ");
        int vendor_id = Integer.parseInt(readLine());

        Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        String query = "SELECT product_id, category_id, product_code, product_name, vendor_id " +
                "FROM product " +
                "ORDER BY category_id ";

        ResultSet rset1 = queryExecution(stmt, query);

        System.out.println("\nProduct List Before Addition ");
        System.out.println("----------------------------------");

        printResults(rset1);

        PreparedStatement insert = conn.prepareStatement
                ("INSERT INTO product (product_id, category_id, product_code, product_name, description, " +
                        "list_price, discount_percent, date_added, vendor_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ");

        insert.setInt(1, product_id);
        insert.setInt(2, category_id);
        insert.setString(3, product_code);
        insert.setString(4, product_name);
        insert.setString(5, description);
        insert.setFloat(6, list_price);
        insert.setFloat(7, discount_percent);
        insert.setString(8, date_added);
        insert.setInt(9, vendor_id);

        insert.executeLargeUpdate();

        ResultSet rset2 = queryExecution(stmt, query);

        System.out.println("\nProduct List After Addition ");
        System.out.println("---------------------------------");

        printResults(rset2);

        stmt.close();

    }


    // QUERY 13
    private static void deleteAproduct(Connection conn) throws SQLException, IOException
    {
        /**
         * Displays and deletes a product info
         * */

        System.out.print("Enter the Product Id of the Product To Be Deleted: ");
        int product_id = Integer.parseInt(readLine());

        Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        String query = "SELECT product_id, category_id, product_code, product_name, vendor_id " +
                "FROM product " +
                "ORDER BY category_id ";

        ResultSet rset1 = queryExecution(stmt, query);

        System.out.println("\nProduct List Before Deletion ");
        System.out.println("-------------------------------");

        printResults(rset1);

        PreparedStatement delete = conn.prepareStatement
                ("DELETE FROM product " +
                        "WHERE product_id IN (?) ");

        delete.setInt(1, product_id);

        delete.executeLargeUpdate();

        ResultSet rset2 = queryExecution(stmt, query);

        System.out.println("\nProduct List After Deletion ");
        System.out.println("------------------------------");

        printResults(rset2);

        stmt.close();

    }


    // PRINT MENU
    private static void printMenu()
    {
        /**
         * Prints the menu to choose from
         * */

        System.out.println("\n\tQUERY OPTIONS ");
        System.out.println("(1) Category Names ");
        System.out.println("(2) Product Names and List Prices ");
        System.out.println("(3) Customer Emails, Order Ids and Total Amounts ");
        System.out.println("(4) Customer Emails, Order Ids, and Order Dates ");
        System.out.println("(5) Product Id, Product Name, and List Price ");
        System.out.println("(6) Customers Who Spent More Than $300 ");
        System.out.println("(7) First 3 Orders ");
        System.out.println("(8) Customers Who Buy From Vendors H% ");
        System.out.println("(9) Products Sold 2 or More ");
        System.out.println("(10) Orders with Greater Total Than Order Id 7 ");
        System.out.println("(11) Update Customer Address Info ");
        System.out.println("(12) Add a New Product Info ");
        System.out.println("(13) Delete a Product ");
        System.out.println("(Q) Quit \n");
    }


    // READLINE
    private static String readLine()
    {
        /**
         * Read lines
         * */

        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(isr, 1);
        String line = "";
        try
        {
            line = br.readLine();
        }
        catch (IOException e)
        {
            System.out.println("Error in SimpleIO.readLine: " +
                    "IOException was thrown");
            System.exit(1);
        }
        return line;
    }
}
