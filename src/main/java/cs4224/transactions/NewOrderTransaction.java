package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
<<<<<<< HEAD

public class NewOrderTransaction extends BaseTransaction {
    private int customerId;
    private int warehouseId;
    private int districtId;
    private int noOfItems;

    public NewOrderTransaction(CqlSession session) {
        super(session);
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        customerId = Integer.parseInt(parameters[1]);
        warehouseId = Integer.parseInt(parameters[2]);
        districtId = Integer.parseInt(parameters[3]);
        noOfItems = Integer.parseInt(parameters[4]);

        System.out.printf("Running New Order Transaction with C_ID= %d, W_ID=%d, D_ID=%d, N=%d \n", customerId, warehouseId, districtId, noOfItems);
        for (String line : dataLines) {
            System.out.println(line);
        }
    }
}
=======
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.*;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.*;
import lombok.val;

import java.math.BigDecimal;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.HashSet;

public class NewOrderTransaction extends BaseTransaction {

    private Session session;
    private PreparedStatement selectwhseps;
    private PreparedStatement selectdistrictps;
    private PreparedStatement selectcustomerps;
    private PreparedStatement selectstockps;
    private PreparedStatement selectitemps;

    private PreparedStatement updatedistrictnextorderIDps;
    private PreparedStatement updateCustomerOrderps;
    private PreparedStatement updateStockps;

    private PreparedStatement insertOrderLineps;


    /**
     * private int customerId;
     * private int warehouseId;
     * private int districtId;
     * private int noOfItems;
     * private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
     **/

    //private static final String selectitem = "SELECT I_NAME, I_PRICE FROM item WHERE I_ID = ? ";

    private static final String insertol = "INSERT INTO order_line (" + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_I_NAME, "
            + " OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";



    public NewOrderTransaction(CqlSession session) {
        super(session);

        this.session = session;
        this.selectwhseps = session.prepare("SELECT W_TAX FROM warehouse WHERE W_ID = ?");
        this.selectdistrictps = session.prepare("SELECT D_TAX, D_NEXT_O_ID FROM district WHERE D_W_ID = ? and D_ID = ?");
        this.selectcustomerps = session.prepare("SELECT C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_DISCOUNT FROM customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        //this.selectcustomerps = session.prepare(getCustomer);
        this.selectstockps = session.prepare("SELECT * FROM stock WHERE S_W_ID = ? AND S_I_ID = ? ");
        this.selectitemps = session.prepare("SELECT I_NAME, I_PRICE FROM item WHERE I_ID = ? ");
        //this.selectitemps = session.prepare(selectitem);
        this.updatedistrictnextorderIDps = session.prepare("UPDATE district SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ? ");
        this.updateCustomerOrderps = session.prepare("UPDATE customer SET C_LAST_ORDER = ?, C_ENTRY_D = ?, C_CARRIER_ID = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        this.updateStockps = session.prepare("UPDATE stock SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_W_ID = ? AND S_I_ID = ?");
        this.insertOrderLineps = session.prepare(insertol);
    }


    private Row getCustomer(int whseID, int distID, int custID) {
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet res;
            res = session.execute(selectcustomerps.bind(whseID, distID, custID));
            List<Row> customers = res.all();
            return (!customers.isEmpty() ? customers.get(0) : null);

        }
    }

    private Row getDistrict(int whseID, int distID) {
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet res;
            res = session.execute(selectdistrictps.bind(whseID, distID));
            List<Row> dist = res.all();
            return (!dist.isEmpty() ? dist.get(0) : null);

        }
    }

    private Row getWarehouse(int whseID) {
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet res;
            res = session.execute(selectwhseps.bind(whseID));
            List<Row> whse = res.all();
            return (!whse.isEmpty() ? whse.get(0) : null);
        }

    }

    private String getDistrictStringID(int DistID) {
        int cnt_dist = 10;
        if (DistID < cnt_dist) {
            return "S_DIST_0" + DistID;
        } else {
            return "S_DIST_10";
        }

    }

    private Row selectItem(int itemID) {
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet res;
            res = session.execute(selectitemps.bind(itemID));
            List<Row> items = res.all();
            return (!items.isEmpty()) ? items.get(0) : null;
        }
    }

    private Row selectStock(int item_w_ID, int itemID) {
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet res;
            res = session.execute((selectstockps.bind(item_w_ID, itemID)));
            List<Row> stock = res.all();
            return (!stock.isEmpty()) ? stock.get(0) : null;

        }

    }

    private void updatedistrictnextorderID(int nextorderid, int whseid, int distid) {
        try (CqlSession session = CqlSession.builder().build()) {

            session.execute(updatedistrictnextorderIDps.bind(nextorderid, whseid, distid));
        }
    }

    private void updateStock(int item_w_ID, int itemID, int adjqty_dec, BigDecimal ytd, int ordercnt, int remotecnt) {
        try (CqlSession session = CqlSession.builder().build()) {

            session.execute(updateStockps.bind(item_w_ID, itemID, adjqty_dec, ytd, ordercnt, remotecnt));
        }

    }


    void processOrder(int custID, int whseID, int distID, List<List<Integer>> itemorders) {
        Row customer = getCustomer(whseID, distID, custID);
        Row district = getDistrict(whseID, distID);
        Row warehouse = getWarehouse(whseID);

        double district_tax = district.getBigDecimal("D_TAX").doubleValue();
        double warehouse_tax = warehouse.getBigDecimal("W_TAX").doubleValue();

        int nextOrderID = district.getInt("D_NEXT_O_ID");
        updatedistrictnextorderID(nextOrderID + 1, whseID, distID);

        /***private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");***/

        Date currentdt = new Date();
        BigDecimal olcnt = new BigDecimal(itemorders.size());
        BigDecimal all_local = new BigDecimal(1);
        for (List<Integer> order : itemorders) {
            if (order.get(1) != whseID) {
                all_local = new BigDecimal(0);
            }
        }

        createNewOrder(nextOrderID, distID, whseID, custID, currentdt, olcnt, all_local, customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST"));

        updateCustOrder(nextOrderID, currentdt, whseID, distID, custID);

        double totalAmt = 0.0;
        for (int i = 0; i < itemorders.size(); i++) {
            int itemID = itemorders.get(i).get(0);
            int item_w_ID = itemorders.get(i).get(1);
            int qty = itemorders.get(i).get(2);

            //query to check stock quantity
            Row stock_info = selectStock(item_w_ID, itemID);
            // double adjQty = stock_info.getLong("S_QUANTITY") - qty;
            int adjQty = stock_info.getInt("S_QUANTITY") - qty;

            if (adjQty < 10) {
                adjQty += 100;
            }
            //BigDecimal adjqty_dec = new BigDecimal(adjQty);

            updateStock(item_w_ID, itemID, adjQty, stock_info.getBigDecimal("S_YTD").add(new BigDecimal(qty)), stock_info.getInt("S_ORDER_CNT") + 1, (item_w_ID != whseID) ? stock_info.getInt("S_REMOTE_CNT") + 1 : stock_info.getInt("S_REMOTE_CNT"));

            Row item = selectItem(itemID);
            String item_name = item.getString("I_NAME");
            BigDecimal item_amt = item.getBigDecimal("I_PRICE").multiply(new BigDecimal(qty));
            totalAmt = totalAmt + item_amt.doubleValue();

            createNewOrderLine(whseID, distID, nextOrderID, i, itemID, item_name, item_amt, item_w_ID, new BigDecimal(qty), stock_info.getString(getDistrictStringID(distID)));

            System.out.printf("Item_ID: %d, Item_Name: %s, Warehouse_ID : %d, quantity : %d, OL_AMOUNT: %f, S_QUANTITY: %f", itemID, item_name, item_w_ID, qty, item_amt, adjQty);


        }
        totalAmt = totalAmt * (1 + district_tax + warehouse_tax) * (1 - customer.getBigDecimal("C_DISCOUNT").doubleValue());

        System.out.printf("Customer C_W_ID : %d, C_D_ID : %d, C_ID : %d, C_LAST : %s, C_CREDIT : %s, C_DISCOUNT : %s", whseID, distID, custID, customer.getString("C_LAST"), customer.getString("C_CREDIT"), customer.getBigDecimal("C_DISCOUNT"));

        System.out.printf("Warehouse tax rate : %f, District tax rate : %f", warehouse_tax, district_tax);
        System.out.printf("Order number: %d, entry date: %s", nextOrderID, currentdt);
        System.out.printf("Number of items : %d, total amount for order : %f ", olcnt.intValue(), totalAmt);

    }

    private void createNewOrder(int nextOrderID, int distID, int whseID, int custID, Date currentdt, BigDecimal olcnt, BigDecimal all_local, String custf, String custm, String custl) {
        try (CqlSession session = CqlSession.builder().build()) {


           // session.execute( ?)
        }
    }

    private void createNewOrderLine(int whseID, int distID, int nextOrderID, int itemID, String item_name, BigDecimal item_amt, int item_w_ID, BigDecimal qty, String distid) {
        try (CqlSession session = CqlSession.builder().build()) {


            session.execute(insertOrderLineps.bind(whseID, distID, nextOrderID, itemID, item_name, item_amt, item_w_ID, qty, distid));
        }

    }

    private void updateCustOrder(int nextOrderID, Date currentdt, int whseID, int distID, int custID) {
        try (CqlSession session = CqlSession.builder().build()) {

            session.execute(updateCustomerOrderps.bind(nextOrderID, currentdt, whseID, distID, custID));
        }

    }





}




       /*** System.out.println("Running New Order Transaction with C_ID= %d, W_ID=%d, D_ID=%d, N=%d \n", customerId, warehouseId, districtId, noOfItems);
       // for (String line : dataLines) {
          //  System.out.println(line);***/





>>>>>>> origin/new_order_xact
