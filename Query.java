//  To run the program: javac -g VideoStore.java Query.java
//                      java -cp ".;sqljdbc4.jar" VideoStore joesmith password1

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.FileInputStream;


/**
 * Runs queries against a back-end database
 */
public class Query {
    private String configFilename;
    private Properties configProps = new Properties();

    private String jSQLDriver;
    private String jSQLUrl;
    private String jSQLCustomerUrl;
    private String jSQLUser;
    private String jSQLPassword;

    // DB Connection
    private Connection conn;
    private Connection customerConn;

    // Canned queries

    // LIKE does a case-insensitive match
    private static final String SEARCH_SQL = "select * from movie where name like ? order by id";
    private PreparedStatement searchStatement;

    private static final String DIRECTOR_SET_SQL = "select distinct m.id, d.fname, d.lname from movie m, movie_directors md, directors d "
                                                   + "where m.name like ? and "
                                                   + "md.mid = m.id and "
                                                   + "d.id = md.did order by m.id";
    private PreparedStatement directorSetStatement;

    private static final String ACTOR_SET_SQL = "select distinct m.id, a.fname, a.lname from movie m, casts c, actor a "
                                                + "where m.name like ? and "
                                                + "c.mid = m.id and "
                                                + "a.id = c.pid order by m.id";
    private PreparedStatement actorSetStatement;

    private static final String DIRECTOR_MID_SQL = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement directorMidStatement;

    private static final String ACTOR_MID_SQL = "SELECT distinct a.fname, a.lname "
                                                + "FROM actor a, casts c "
                                                + "WHERE c.mid = ? and a.id = c.pid order by lname";
    private PreparedStatement actorMidStatement;

    /* uncomment, and edit, after your create your own customer database */
    private static final String CUSTOMER_LOGIN_SQL = 
        "SELECT * FROM customers WHERE login = ? and password = ?";
    private PreparedStatement customerLoginStatement;

    private static final String CUSTOMER_NAME_SQL = 
        "SELECT * FROM customers WHERE cid = ?";
    private PreparedStatement customerNameStatement;

    private static final String CUSTOMER_PLAN_SQL = "select A.name, A.max_rentals, A.monthly_fee "
                                                  + "from rental_plans A, has_plan B "
                                                  + "where B.cid = ? and A.pid = B.pid;";
    private PreparedStatement customerPlanStatement;

    private static final String CUSTOMER_RENTALS_SQL = "select * from customer_rentals where cid = ? and status = 'open'";
    private PreparedStatement customerRentalsStatement;

    private static final String RENTAL_PLANS_SQL = "select * from rental_plans";
    private PreparedStatement rentalPlansStatement;

    private static final String VALID_PLAN_SQL = "select * from rental_plans where pid = ?";
    private PreparedStatement validPlanStatement;

    private static final String VALID_MOVIE_SQL = "select * from movie where id = ?";
    private PreparedStatement validMovieStatement;

    private static final String RENTER_ID_SQL = "select cid from customer_rentals where status = 'open' and mid = ?";
    private PreparedStatement renterIdStatement;

    private static final String UPDATE_HAS_PLAN_SQL = "update has_plan set pid = ? where cid = ?";
    private PreparedStatement updateHasPlanStatement;

    private static final String UPDATE_CUSTOMER_RENTALS_SQL = "update customer_rentals set status = 'closed' where cid = ? and mid = ?";
    private PreparedStatement updateCustomerRentalsStatement;

    private static final String INSERT_RENTAL_SQL = "INSERT INTO CUSTOMER_RENTALS (cid, mid, status, checkout_date) "
                                                    + "VALUES (?, ?, 'open', SYSDATETIME());";
    private PreparedStatement insertRentalStatement;

    private static final String BEGIN_TRANSACTION_SQL = 
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
    private PreparedStatement beginTransactionStatement;

    private static final String COMMIT_SQL = "COMMIT TRANSACTION";
    private PreparedStatement commitTransactionStatement;

    private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
    private PreparedStatement rollbackTransactionStatement;

    public Query(String configFilename) {
        this.configFilename = configFilename;
    }

    /**********************************************************/
    /* Connection code to SQL Azure. Example code below will connect to the imdb database on Azure
       IMPORTANT NOTE:  You will need to create (and connect to) your new customer database before 
       uncommenting and running the query statements in this file .
     */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream(configFilename));

        jSQLDriver     = configProps.getProperty("videostore.jdbc_driver");
        jSQLUrl        = configProps.getProperty("videostore.imdb_url");
        jSQLUser       = configProps.getProperty("videostore.sqlazure_username");
        jSQLPassword   = configProps.getProperty("videostore.sqlazure_password");

        jSQLCustomerUrl = configProps.getProperty("videostore.customer_url");


        /* load jdbc drivers */
        Class.forName(jSQLDriver).newInstance();

        /* open connections to the imdb database */

        conn = DriverManager.getConnection(jSQLUrl, // database
                                           jSQLUser, // user
                                           jSQLPassword); // password

        conn.setAutoCommit(true); //by default automatically commit after each statement 

        /* You will also want to appropriately set the 
                   transaction's isolation level through: */
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        /* Also you will put code here to specify the connection to your
           customer DB.  E.g. */

        customerConn = DriverManager.getConnection(jSQLCustomerUrl, // database
                                                   jSQLUser, // user
                                                   jSQLPassword); // password
        customerConn.setAutoCommit(true); //by default automatically commit after each statement
        customerConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
   }

    public void closeConnection() throws Exception {
        conn.close();
        customerConn.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        directorMidStatement = conn.prepareStatement(DIRECTOR_MID_SQL);
        validMovieStatement = conn.prepareStatement(VALID_MOVIE_SQL);
        searchStatement = conn.prepareStatement(SEARCH_SQL);
        actorMidStatement = conn.prepareStatement(ACTOR_MID_SQL);
        directorSetStatement = conn.prepareStatement(DIRECTOR_SET_SQL);
        actorSetStatement = conn.prepareStatement(ACTOR_SET_SQL);

        /* uncomment after you create your customers database */
        customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);
        beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
        commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
        rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);

        /* add here more prepare statements for all the other queries you need */
        customerNameStatement = customerConn.prepareStatement(CUSTOMER_NAME_SQL);
        customerPlanStatement = customerConn.prepareStatement(CUSTOMER_PLAN_SQL);
        customerRentalsStatement = customerConn.prepareStatement(CUSTOMER_RENTALS_SQL);
        rentalPlansStatement = customerConn.prepareStatement(RENTAL_PLANS_SQL);
        updateHasPlanStatement = customerConn.prepareStatement(UPDATE_HAS_PLAN_SQL);
        validPlanStatement = customerConn.prepareStatement(VALID_PLAN_SQL);
        renterIdStatement = customerConn.prepareStatement(RENTER_ID_SQL);
        insertRentalStatement = customerConn.prepareStatement(INSERT_RENTAL_SQL);
        updateCustomerRentalsStatement = customerConn.prepareStatement(UPDATE_CUSTOMER_RENTALS_SQL);
    }


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */

    public int getRemainingRentals(int cid) throws Exception {
        /* How many movies can she/he still rent?
           You have to compute and return the difference between the customer's plan
           and the count of outstanding rentals */
        int curRentals = 0;
        int maxRentals = 0;
        int remainingRentals = 0;

        customerRentalsStatement.clearParameters();
        customerRentalsStatement.setInt(1,cid);
        ResultSet customer_set = customerRentalsStatement.executeQuery();
        while (customer_set.next())
        {
            curRentals++;
        }
        customer_set.close();

        customerPlanStatement.clearParameters();
        customerPlanStatement.setInt(1,cid);
        ResultSet plan_set = customerPlanStatement.executeQuery();
        if (plan_set.next())
        {
            maxRentals = plan_set.getInt("max_rentals");
        }
        plan_set.close();

        remainingRentals = maxRentals - curRentals;

        return (remainingRentals);
    }

    public String getCustomerName(int cid) throws Exception {
        /* Find the first and last name of the current customer. */
        String firstName = new String();
        String lastName = new String();

        customerNameStatement.clearParameters();
        customerNameStatement.setInt(1,cid);
        ResultSet customer_set = customerNameStatement.executeQuery();
        if (customer_set.next())
        {
            firstName = customer_set.getString("fname");
            lastName = customer_set.getString("lname");
        }
        customer_set.close();

        return (firstName + " " + lastName);

    }

    public boolean isValidPlan(int planid) throws Exception {
        /* Is planid a valid plan ID?  You have to figure it out */
        validPlanStatement.clearParameters();
        validPlanStatement.setInt(1,planid);
        ResultSet plan_set = validPlanStatement.executeQuery();

        if(plan_set.next())
        {
            plan_set.close();
            return true;
        }
        else
        {
            plan_set.close();
            return false;
        }
    }

    public boolean isValidMovie(int mid) throws Exception {
        /* is mid a valid movie ID?  You have to figure it out */
        validMovieStatement.clearParameters();
        validMovieStatement.setInt(1,mid);
        ResultSet movie_set = validMovieStatement.executeQuery();

        if(movie_set.next())
        {
            movie_set.close();
            return true;
        }
        else
        {
            movie_set.close();
            return false;
        }
    }

    private int getRenterID(int mid) throws Exception {
        /* Find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        renterIdStatement.clearParameters();
        renterIdStatement.setInt(1,mid);
        ResultSet rental_set = renterIdStatement.executeQuery();
        if(rental_set.next())
        {
            int cid = rental_set.getInt("cid");
            rental_set.close();
            return cid;
        }
        else
        {
            rental_set.close();
            return -1;
        }
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */

        /* Uncomment after you create your own customers database */
        int cid;

        customerLoginStatement.clearParameters();
        customerLoginStatement.setString(1,name);
        customerLoginStatement.setString(2,password);
        ResultSet cid_set = customerLoginStatement.executeQuery();
        if (cid_set.next())
        {
            cid = cid_set.getInt(1);
        }
        else
        {
            cid = -1;
        }
        cid_set.close();
        return(cid);
    }

    public void transaction_printPersonalData(int cid) throws Exception {

        String planName = new String();
        int maxRentals = 0;
        float monthlyFee = 0;

        customerPlanStatement.clearParameters();
        customerPlanStatement.setInt(1,cid);
        ResultSet plan_set = customerPlanStatement.executeQuery();
        if (plan_set.next())
        {
            planName = plan_set.getString("name");
            maxRentals = plan_set.getInt("max_rentals");
            monthlyFee = plan_set.getFloat("monthly_fee");
        }
        plan_set.close();

        int remainingRentals = getRemainingRentals(cid);
        int currentRentals = maxRentals - remainingRentals;


        /* println the customer's personal data: name, and plan number */
        System.out.println("********** User Info **********");
        System.out.println(String.format("%-22s%d","[cid]: ", cid));
        System.out.println(String.format("%-22s%s","[User Name]: ", getCustomerName(cid)));
        System.out.println(String.format("%-22s%s","[Plan Name]: ", planName));
        System.out.println(String.format("%-22s%.2f", "[Monthly Fee]: ", monthlyFee));
        System.out.println(String.format("%-22s%d","[Max Rentals]: ", maxRentals));
        System.out.println(String.format("%-22s%d","[Current Rentals]: ", currentRentals));
        System.out.println(String.format("%-22s%d","[Remaining Rentals]: ", remainingRentals));
        System.out.println("*******************************");
    }


    /**********************************************************/
    /* main functions in this project: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

        /* Interpolate the movie title into the SQL string */
        searchStatement.clearParameters();
        searchStatement.setString(1,"%" + movie_title + "%");
        ResultSet movie_set = searchStatement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            directorMidStatement.clearParameters();
            directorMidStatement.setInt(1, mid);
            ResultSet director_set = directorMidStatement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();

            /* now you need to retrieve the actors, in the same manner */
            actorMidStatement.clearParameters();
            actorMidStatement.setInt(1, mid);
            ResultSet actor_set = actorMidStatement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\tActor: " + actor_set.getString("fname")
                                   + " " + actor_set.getString("lname"));
            }
            actor_set.close();

            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
            int rid = getRenterID(mid);
            if(rid == -1)
            {
                System.out.println("\t\tMovie availability: AVAILABLE");
            }
            else if(rid == cid)
            {
                System.out.println("\t\tMovie availability: YOU HAVE IT");
            }
            else
            {
                System.out.println("\t\tMovie availability: UNAVAILABLE");
            }
        }
        movie_set.close();
        System.out.println();
    }

    public void transaction_choosePlan(int cid, int pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
        /* remember to enforce consistency ! */

        updateHasPlanStatement.clearParameters();
        updateHasPlanStatement.setInt(1, pid);
        updateHasPlanStatement.setInt(2, cid);

        beginTransaction();

        int curMaxRentals = 0;
        int newMaxRentals = 0;
        int remainingRentals = 0;
        int currentRentals = 0;
        boolean validPlan = isValidPlan(pid);

        customerPlanStatement.clearParameters();
        customerPlanStatement.setInt(1,cid);
        ResultSet plan_set = customerPlanStatement.executeQuery();
        if (plan_set.next())
        {
            curMaxRentals = plan_set.getInt("max_rentals");
        }
        plan_set.close();

        remainingRentals = getRemainingRentals(cid);
        currentRentals = curMaxRentals - remainingRentals;

        validPlanStatement.clearParameters();
        validPlanStatement.setInt(1,pid);
        ResultSet rental_set = validPlanStatement.executeQuery();
        if (rental_set.next())
        {
            newMaxRentals = rental_set.getInt("max_rentals");
        }
        rental_set.close();

        updateHasPlanStatement.executeUpdate();

        if(validPlan && (newMaxRentals >= currentRentals))
        {
            System.out.println("Commit transaction");
            commitTransaction();
        }
        else
        {
            System.out.println("Rollback transaction");
            rollbackTransaction();
        }
    }

    public void transaction_listPlans() throws Exception {
        /* println all available plans: SELECT * FROM plan */
        rentalPlansStatement.clearParameters();
        ResultSet plan_set = rentalPlansStatement.executeQuery();
        System.out.println("********************** Plans **********************");
        System.out.println(String.format("%-10s%-15s%-14s%s","pid","name","max_rentals","monthly_fee"));
        while (plan_set.next())
        {
            int pid = plan_set.getInt("pid");
            String name = plan_set.getString("name");
            int maxRentals = plan_set.getInt("max_rentals");
            float monthlyFee = plan_set.getFloat("monthly_fee");

            System.out.println(String.format("%-10d%-15s%-14d$%.2f",pid,name,maxRentals,monthlyFee));
        }
        System.out.println("***************************************************");
        plan_set.close();
    }

    public void transaction_rent(int cid, int mid) throws Exception {
        /* rent the movie mid to the customer cid */
        /* remember to enforce consistency ! */
        beginTransaction();

        int remainingRentals = getRemainingRentals(cid);
        int renterId = getRenterID(mid);
        boolean validMovie = isValidMovie(mid);

        insertRentalStatement.clearParameters();
        insertRentalStatement.setInt(1, cid);
        insertRentalStatement.setInt(2, mid);
        insertRentalStatement.executeUpdate();

        if(remainingRentals > 0 && renterId == -1 && validMovie)
        {
            System.out.println("Commit transaction");
            commitTransaction();
        }
        else
        {
            System.out.println("Rollback transaction");
            rollbackTransaction();
        }
    }

    public void transaction_return(int cid, int mid) throws Exception {
        /* return the movie mid by the customer cid */

        beginTransaction();
        int renterId = getRenterID(mid);

        updateCustomerRentalsStatement.clearParameters();
        updateCustomerRentalsStatement.setInt(1, cid);
        updateCustomerRentalsStatement.setInt(2, mid);
        updateCustomerRentalsStatement.executeUpdate();
        if(renterId == cid)
        {
            System.out.println("Commit transaction");
            commitTransaction();
        }
        else
        {
            System.out.println("Rollback transaction");
            rollbackTransaction();
        }
    }

    public void transaction_fastSearch(int cid, String movie_title)
            throws Exception {
        /* like transaction_search, but uses joins instead of dependent joins
           Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
           Answers are sorted by mid.
           Then merge-joins the three answer sets */

        // Part a) Search for movies
        searchStatement.clearParameters();
        searchStatement.setString(1,"%" + movie_title + "%");
        ResultSet movie_set = searchStatement.executeQuery();

        directorSetStatement.clearParameters();
        directorSetStatement.setString(1,"%" + movie_title + "%");
        ResultSet director_set = directorSetStatement.executeQuery();

        actorSetStatement.clearParameters();
        actorSetStatement.setString(1,"%" + movie_title + "%");
        ResultSet actor_set = actorSetStatement.executeQuery();

        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));

            while(director_set.next() && director_set.getInt(1) == mid)
            {
                System.out.println("\t\tDirector: " + director_set.getString("fname")
                        + " " + director_set.getString("lname"));
            }

            while(actor_set.next() && actor_set.getInt(1) == mid)
            {
                System.out.println("\t\tActor: " + actor_set.getString("fname")
                                   + " " + actor_set.getString("lname"));
            }
        }

        movie_set.close();
        director_set.close();
        actor_set.close();
    }


    /* Uncomment helpers below once you've got beginTransactionStatement,
       commitTransactionStatement, and rollbackTransactionStatement setup from
       prepareStatements():
    */
    public void beginTransaction() throws Exception
    {
        customerConn.setAutoCommit(false);
        beginTransactionStatement.executeUpdate();
    }

    public void commitTransaction() throws Exception
    {
        commitTransactionStatement.executeUpdate();
        customerConn.setAutoCommit(true);
    }

    public void rollbackTransaction() throws Exception
    {
        rollbackTransactionStatement.executeUpdate();
        customerConn.setAutoCommit(true);
    }

}
