package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

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
    private  PreparedStatement selectitemps;

    private PreparedStatement updatedistrictnextorderIDps;
    private PreparedStatement updateCustomerOrderps;
    private PreparedStatement updateStockps;

    private PreparedStatement insertOrderLineps;
    private PreparedStatement insertOrderByIdps;

    /**
     * private int customerId;
     * private int warehouseId;
     * private int districtId;
     * private int noOfItems;
     private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
     **/

    private static final String selectitem = "SELECT I_NAME, I_PRICE FROM item WHERE I_ID = ? ";


    public NewOrderTransaction(CqlSession session) {
        super(session);

        this.session = session;
        this.selectwhseps = session.prepare("SELECT W_TAX FROM warehouse WHERE W_ID = ?");
        this.selectdistrictps = session.prepare("SELECT D_TAX, D_NEXT_O_ID FROM district WHERE D_W_ID = ? and D_ID = ?");
        this.selectcustomerps = session.prepare("SELECT C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_DISCOUNT FROM customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        this.selectstockps = session.prepare("SELECT * FROM stock WHERE S_W_ID = ? AND S_I_ID = ? ");
        //this.selectitemps = session.prepare("SELECT I_NAME, I_PRICE FROM item WHERE I_ID = ? ");
        this.selectitemps = session.prepare(selectitem);
        this.updatedistrictnextorderIDps = session.prepare("UPDATE district SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ? ");
        //this.updateCustomerOrderps = session.prepare("UPDATE customer SET C_LAST_ORDER = ?, C_ENTRY_D = ?, C_CARRIER_ID = ? WHERE C_W_ID ? AND C_D_ID = ? AND C_ID = ?");
        this.updateStockps = session.prepare("UPDATE stock SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_W_ID = ? AND S_I_ID = ?");
    }

    void processOrder(int custID, int whseID, int distID, List<List<Integer>> itemorders) {
        Row customer = getCustomer(whseID, distID, custID);
        Row district = getDistrict(whseID, distID);
        Row warehouse = getWarehouse(whseID);

        double district_tax = district.getBigDecimal("D_TAX").doubleValue();
        double warehouse_tax = warehouse.getBigDecimal("W_TAX").doubleValue();

        int nextOrderID = district.getInt("D_NEXT_O_ID");
        updatedistrictnextorderID(nextOrderID +1 , whseID, distID);

        /***private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");***/

        Date currentdt =new Date();
        BigDecimal olcnt = new BigDecimal(itemorders.size());
        BigDecimal all_local = new BigDecimal(1);
        for(List<Integer>order : itemorders)
        {
            if (order.get(1) != whseID)
            {
                all_local = new BigDecimal(0);
            }
        }

        createNewOrder(nextOrderID, distID, whseID, custID, currentdt,olcnt, all_local, customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST"));

        updateCustOrder(nextOrderID, currentdt, whseID, distID, custID);

        double totalAmt = 0.0;
        for (int i= 0; i <itemorders.size(); i++)
        {
            int itemID = itemorders.get(i).get(0);
            int item_w_ID = itemorders.get(i).get(1);
            int qty = itemorders.get(i).get(2);

            //query to check stock quantity
            Row stock_info = selectStock(item_w_ID, itemID);
            double adjQty = stock_info.getLong("S_QUANTITY") - qty;

            if (adjQty < 10)
            {
                adjQty +=100;
            }
            BigDecimal adjqty_dec = new BigDecimal(adjQty);

            updateStock(item_w_ID, itemID, adjqty_dec, stock_info.getBigDecimal("S_YTD").add(new BigDecimal(qty)), stock.getInt("S_ORDER_CNT") + 1, (item_w_ID != whseID) ? stock.getInt("S_REMOTE_CNT") + 1 : stock.getInt("S_REMOTE_CNT"));

            Row item = selectItem(itemID);
            String item_name = item.getString("I_NAME");
            BigDecimal item_amt = item.getBigDecimal("I_PRICE").multiply(new BigDecimal(qty));
            totalAmt = totalAmt + item_amt.doubleValue();

            createNewOrderLine(whseID, distID, nextOrderID, i, itemID, item_name, item_amt, item_w_ID, new BigDecimal(qty), stock.getString(getDistrictStringID(distID)));

            System.out.printf("Item_ID: %d, Item_Name: %s, Warehouse_ID : %d, quantity : %d, OL_AMOUNT: %f, S_QUANTITY: %f", itemID, item_name, item_w_ID, qty, item_amt, adjQty);


        }
        totalAmt = totalAmt * (1 + district_tax + warehouse_tax) * (1 - customer.getBigDecimal("C_DISCOUNT").doubleValue());

        System.out.printf("Customer C_W_ID : %d, C_D_ID : %d, C_ID : %d, C_LAST : %s, C_CREDIT : %s, C_DISCOUNT : %s", whseID, distID, custID, customer.getString("C_LAST"), customer.getString("C_CREDIT"), customer.getBigDecimal("C_DISCOUNT"));

        System.out.printf("Warehouse tax rate : %f, District tax rate : %f", warehouse_tax, district_tax);
        System.out.printf("Order number: %d, entry date: %s", nextOrderID, currentdt);
        System.out.printf("Number of items : %d, total amount for order : %f ", olcnt.intValue(), totalAmt);

    }

    private String getDistrictStringID(int DistID) {
    if (DistID < 10)
    {
        return "S_DIST_0" + DistID;
    }
    else
    {
        return "S_DIST_10";
    }

    }

    private Row selectItem(int itemID)
    {
        ResultSet res;
        res = session.execute(selectitemps.bind(itemID));
        List<Row> items = res.all();
        return (!items.isEmpty()) ? items.get(0) : null;
    }

}



       /*** System.out.println("Running New Order Transaction with C_ID= %d, W_ID=%d, D_ID=%d, N=%d \n", customerId, warehouseId, districtId, noOfItems);
       // for (String line : dataLines) {
          //  System.out.println(line);***/





