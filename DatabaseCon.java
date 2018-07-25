package com.adb.bidding;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author metamug
 */
public class DatabaseCon {

    public String getNextMsgSellerId() {
        String next;
        Connection con;
        try {
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select max(coalesce(autospid,100))+1 from problem_sellers");
            rs.next();
            next = rs.getString(1);
            rs.close();
            stmt.close();
            closeConnection(con);
            return (next);
        } catch (SQLException ex) {
            System.out.println("Error in Generating nextmsg id");
            return ("NA");
        }
    }

    public String getWinner(String itemid) {
        String result;
        Connection con;
        try {
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select coalesce(buyerid,'No result') from item where itemid like '" + itemid + "'");
            rs.next();

            result = rs.getString(1);
            rs.close();
            stmt.close();
            closeConnection(con);
            return (result);
        } catch (SQLException ex) {
            System.out.println("Error in getting winner ");
            return ("na");
        }
    }

    public String getNextMsgBuyerId() {
        String next;
        Connection con;
        try {
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*)+100 from problem_buyers");
            rs.next();
            next = rs.getString(1);
            rs.close();
            stmt.close();
            closeConnection(con);

            return (next);
        } catch (SQLException ex) {
            System.out.println("Error in Generating nextmsg id");
            return ("NA");
        }
    }

    public static ArrayList getMineWinners(String bid) {
        ArrayList<String> al = null;
        Connection con;

        try {
            al = new ArrayList<>();
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select bidid,b.itemid,b.buyerid,bidamount from bidmaster b,item w where b.buyerid='" + bid + "' and w.buyerid is not null");
            while (rs.next()) {
                al.add(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4));
            }
            rs.close();
            stmt.close();
            closeConnection(con);
            return (al);

        } catch (SQLException ex) {
            System.out.println("Error in Getting History  " + bid);
            return (al);
        }
    }

    public ArrayList getMyProblems(String adminid) {
        ArrayList<String> al = null;
        Connection con;

        try {
            al = new ArrayList<>();
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from problem_sellers where adminid like '" + adminid + "'");
            while (rs.next()) {
                al.add(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5));
            }

            stmt = con.createStatement();
            rs = stmt.executeQuery("select * from problem_buyers where adminid like '" + adminid + "'");

            while (rs.next()) {
                al.add(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5));
            }
            rs.close();
            stmt.close();
            closeConnection(con);
            return (al);

        } catch (SQLException ex) {
            System.out.println("Error in Getting Problems " + adminid);
            return (al);
        }
    }

    /**
     * Get Rows in the sql result
     *
     * @param sql
     * @return
     */
    public int getRowCount(String sql) {
        Connection con;
        int cnt;
        try {
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            cnt = rs.getInt(1);
            rs.close();
            stmt.close();
            closeConnection(con);
            return (cnt);
        } catch (SQLException ex) {
            System.out.println("Error " + ex.getMessage());
            return (-1);
        }
    }

    public int getProblemCount() {
        int cnt = 0;
        Connection con;
        try {
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("Select count(*) from problem_sellers");
            rs.next();
            cnt += rs.getInt(1);

            stmt = con.createStatement();
            rs = stmt.executeQuery("Select count(*) from problem_buyers");
            rs.next();
            cnt += rs.getInt(1);

            stmt.close();
            rs.close();
            closeConnection(con);
            return (cnt);
        } catch (SQLException ex) {
            System.out.println("Error from problems count " + ex);
            return (-1);
        }
    }

    public String getNextId(String catg) {
        String result;
        Connection con;
        try {
            con = getconnection();
            Statement stmt = con.createStatement();
            ResultSet rs;
            if (catg.startsWith("Se")) {
                rs = stmt.executeQuery("select count(*)+1 from seller");
            } else if (catg.startsWith("Bu")) {
                rs = stmt.executeQuery("select count(*)+1 from buyer");
            } else {
                rs = stmt.executeQuery("select count(*)+1 from administrator");
            }

            rs.next();
            result = catg + "" + rs.getInt(1);
            stmt.close();
            rs.close();
            closeConnection(con);
            return (result);
        } catch (SQLException ex) {
            System.out.println("Error in generating Id for Users ");
            return ("NA");
        }
    }

    public static Connection getconnection() {
        Connection con = null;
        try {

            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/auctiondb", "root", "test");
        } catch (ClassNotFoundException | SQLException ex) {
            System.out.println("class error" + ex);
        }
        return con;
    }

    public static void closeConnection(Connection con) {
        try {
            if (con != null) {
                con.close();
            }
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }

}
